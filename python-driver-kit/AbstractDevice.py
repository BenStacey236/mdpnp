from ice import ice_Alert
from ice import ice_DeviceAlertCondition
from ice import ice_DeviceIdentity
#from ice import ice_DeviceIdentityDataWrite
#from ice import ice_DeviceIdentityTypeSupport
from ice import ice_AlarmLimit
from ice import ice_LimitType
from ice import ice_LocalAlarmLimitObjective
from ice import ice_Numeric
#from ice import ice_NumericTypeSupport
from ice import ice_SampleArray
#from ice import ice_SampleArrayDataWriter
#from ice import ice_SampleArrayTypeSupport
from ice_DataWriter import ice_DataWriter

import DeviceClock
from DomainClock import DomainClock

import units
from units import rosetta

from rti.connextdds import DomainParticipant
#from rti.connextdds import infrastructure.Condition
from rti.connextdds import InstanceHandle
#from rti.connextdds import infrastructure.RETCODE_NO_DATA
#from rti.connextdds import infrastructure.ResourceLimitsQosPolicy
#from rti.connextdds import infrastructure.StatusKind
from rti.connextdds import Time
#from rti.connextdds import publication.Publisher
#from rti.connextdds import subscription.InstanceStateKind
#from rti.connextdds import subscription.ReadCondition
#from rti.connextdds import subscription.SampleInfo
#from rti.connextdds import subscription.SampleInfoSeq
#from rti.connextdds import subscription.SampleStateKind
from rti.connextdds import Subscriber
#from rti.connextdds import ViewStateKind
#from rti.connextdds import Topic

from abc import ABC, abstractmethod
from overrides import overrides
from typing import TypeVar, Final, Generic, Optional, Collection, Iterator
import logging
import threading

T = TypeVar('T') # Used for typing generics


class InstanceHolder(Generic[T]):
    """
    InstanceHolder class that is Generic on type T, used by AbstractDevice
    """

    def __init__(self, data: Optional[T] = None, handle: Optional[InstanceHandle] = None):
        self.data = data
        self.handle = handle

    def __str__(self) -> str:
        return f"[data={self.data},handle={self.handle}]"


class Averager:
    """
    A class for keeping a rolling array to derive an average from.
    It uses an python list to keep the values, rather than a fixed size
    array, to cater for variable frequency of data.  Calling the get()
    method empties the array.  Adding via the add() method or calculating
    the average use python thread locking to ensure thread safety, as the 
    average will be requested from a different thread to one populating values
    """

    def __init__(self):
        """
        Initialises a new `Averager` instance
        """

        self.__values = []
        self.__lock = threading.Lock()
    

    def add(self, value: float) -> None:
        """
        Add a new value to the internal list

        :param value: The value to add to the internal list
        """

        with self.__lock:
            self.__values.append(value)
    

    def get(self) -> float:
        """
        Gets the average of the values stored in the internal list

        :returns average: The average of all values in the internal list
        """

        with self.__lock:
            # Avoid division by zero
            if not self.__values:
                return 0.0
            
            avg = sum(self.__values) / len(self.__values)
            self.__values.clear()
            return avg


class NullSaveContainer(ABC, Generic[T]):
    """
    Generic wrapper/container interface mainly used for constructing sample arrays
    """

    @abstractmethod
    def is_null() -> bool:
        """
        Returns whether or not the current container is None
        """
        pass

    @abstractmethod
    def __iter__() -> Iterator[T]:
        """
        Returns an iterator over the elements in the container.
        """
        pass


    @abstractmethod
    def size() -> int:
        """
        Return the number of elements in the container.
        """
        pass


class CollectionContainer(NullSaveContainer[T], Generic[T]):
    """
    A generic container class that wraps objects classed as a `Collection`
    """

    def __init__(self, data: Collection[T]) -> None:
        """
        Initialises a new `CollectionContainer` instance

        :param data: A `Collection` for the container to store
        """

        self.__dt: Final[Collection[T]] = data


    @overrides
    def is_null(self) -> bool:
        return self.__dt is None
    

    @overrides
    def __iter__(self) -> Iterator[T]:
        if self.__dt is None:
            return iter()
        return iter(self.__dt)
    

    @overrides
    def size(self) -> int:
        return 0 if self.__dt is None else len(self.__dt)


class ArrayContainer(NullSaveContainer[T], Generic[T]):
    """
    A container class that wraps arrays containing generic type T
    """

    def __init__(self, data: list[T], length: int = None) -> None:
        """
        Initialises a new `ArrayContainer` instance

        :param data: An array containing the data of datatype T
        :param length: Optionally, the length of the array.
        """

        self.__dt: Final[list[T]] = data
        self.__l: Final[int] = 0 if data is None else len(data)


    @overrides
    def is_null(self) -> bool:
        return self.__dt is None
    

    overrides
    def __iter__(self) -> Iterator[T]:
        if self.__dt is None:
            return iter()
        return iter(self.__dt[:self.__l])
    

    @overrides
    def size(self) -> int:
        return self.__l


class AbstractDevice(ABC):
    """
    Python equivalent to the AbstractDevice Java class. All other OpenICE devices 
    inherit from this class.
    """

    # Static variables
    __log: Final[logging.Logger] = logging.getLogger("AbstractDevice")


    def __init__(self) -> None:
        # TODO: UPDATE THE CONSTRUCTOR TO MATCH ACTUAL AbstractDevice CONSTRUCTOR

        self._domainParticipant: Final[DomainParticipant] = None
        self._subscriber: Final[Subscriber] = None
        self._deviceIdentity: Final[ice_DeviceIdentity] = None

        self._numericDataWriter: Final[ice_DataWriter[ice_Numeric]] = None

        self._sampleArrayDataWriter: Final[ice_DataWriter[ice_SampleArray]] = None

        self._alarmLimitDataWriter: Final[ice_DataWriter[ice_AlarmLimit]] = None

        self._alarmLimitObjectiveDataWriter: Final[ice_DataWriter[ice_LocalAlarmLimitObjective]] = None

        self._deviceAlertConditionDataWriter: ice_DataWriter[ice_DeviceAlertCondition] = None

        self._patientAlertWriter: ice_DataWriter[ice_Alert] = None

        self._technicalAlertWriter: ice_DataWriter[ice_Alert] = None

        self._deviceAlertConditionInstance: InstanceHolder[ice_DeviceAlertCondition] = None

        self.__averagesByNumeric: dict[str, Averager] = {}

        self.__registeredSampleArrayInstances: Final[list[InstanceHolder[ice_SampleArray]]] = []
        self.__registeredNumericInstances: Final[list[InstanceHolder[ice_Numeric]]] = []
        self.__registeredAlarmLimitInstances: Final[list[InstanceHolder[ice_AlarmLimit]]] = []
        self.__registeredAlarmLimitObjectiveInstances: Final[list[InstanceHolder[ice_LocalAlarmLimitObjective]]] = []
        self.__patientAlertInstances: Final[dict[str, InstanceHolder[ice_Alert]]] = {}
        self.__technicalAlertInstances: Final[dict[str, InstanceHolder[ice_Alert]]] = {}
        self.__oldPatientAlertInstances: Final[set[str]] = set()
        self.__oldTechnicalAlertInstances: Final[set[str]] = set()


    def getSubscriber(self) -> Subscriber:
        """
        :returns subscriber: The current device's `Subscriber` instance
        """

        return self._subscriber


    def getDeviceIdentity(self) -> ice_DeviceIdentity:
        """
        :returns deviceIdentity: The current device's `DeviceIdentity` instance
        """

        return self._deviceIdentity
    

    def getParticipant(self) -> DomainParticipant:
        """
        :returns domainParticipant: The current device's `DomainParticipant` instance
        """

        return self._domainParticipant
    

    def _createNumericInstance(self, metric_id: str, vendor_metric_id: str, instance_id: int = 0, unit_id: str = units.rosetta_MDC_DIM_DIMLESS) -> InstanceHolder[ice_Numeric]:
        """
        Creates an initial `Numeric` instance and registers it on DDS

        :param metric_id: The metric_id of the new `Numeric` instance
        :param vendor_metric_id: The vendor_metric_id of the new `Numeric` instance
        :param instance_id: The instance_id of the new `Numeric` instance
        :param unit_id: The unit_id of the new `Numeric` instance
        :returns holder: The `InstanceHolder` of the `Numeric` instance
        :raises RuntimeError: When `_deviceIdentity.unique_device_identifier` is not populated before calling
        """

        if (not self._deviceIdentity or not self._deviceIdentity.unique_device_identifier):
            raise RuntimeError("Please populate _deviceIdentity.unique_device_identifier before calling _createNumericInstance")
        
        holder: InstanceHolder[ice_Numeric] = InstanceHolder()
        holder.data = ice_Numeric()
        holder.data.unique_device_identifier = self._deviceIdentity.unique_device_identifier
        holder.data.metric_id = metric_id
        holder.data.vendor_metric_id = vendor_metric_id
        holder.data.instance_id = instance_id
        holder.data.unit_id = unit_id

        holder.handle = self._numericDataWriter.register_instance(holder.data)
        if holder.handle.is_nil:
            self.__log.warning(f"Unable to register instance: {holder.data}")
            holder.handle = None
        else:
            self.__registeredNumericInstances.append(holder)

        return holder
    

    def _createAlarmLimitInstance(self, metric_id: str, limit_type: ice_LimitType) -> InstanceHolder[ice_AlarmLimit]:
        """
        Creates an `AlarmLimit` instance and registers it on DDS

        :param metric_id: The metric_id of the new `AlarmLimit` instance
        :param limit_type: The `LimitType` of the new `AlarmLimit` instance
        :returns holder: The `InstanceHolder` of the `AlarmLimit` instance
        :raises RuntimeError: When `_deviceIdentity.unique_device_identifier` is not populated before calling
        """

        if (not self._deviceIdentity or not self._deviceIdentity.unique_device_identifier):
            raise RuntimeError("Please populate _deviceIdentity.unique_device_identifier before calling _createAlarmLimitInstance")
        
        holder: InstanceHolder[ice_AlarmLimit] = InstanceHolder()
        holder.data = ice_AlarmLimit()
        holder.data.unique_device_identifier = self._deviceIdentity.unique_device_identifier
        holder.data.metric_id = metric_id
        holder.data.limit_type = limit_type

        holder.handle = self._alarmLimitDataWriter.register_instance(holder.data)
        if holder.handle.is_nil:
            self.__log.warning(f"Unable to register instance: {holder.data}")
            holder.handle = None
        else:
            self.__registeredAlarmLimitInstances.append(holder)
        
        return holder
    

    def _createAlarmLimitObjectiveInstance(self, metric_id: str, limit_type: ice_LimitType) -> InstanceHolder[ice_LocalAlarmLimitObjective]:
        """
        Creates an `LocalAlarmLimitObjective` instance and registers it on DDS

        :param metric_id: The metric_id of the new `LocalAlarmLimitObjective` instance
        :param limit_type: The `LimitType` of the new `LocalAlarmLimitObjective` instance
        :returns holder: The `InstanceHolder` of the `LocalAlarmLimitObjective` instance
        :raises RuntimeError: When `_deviceIdentity.unique_device_identifier` is not populated before calling
        """

        if (not self._deviceIdentity or not self._deviceIdentity.unique_device_identifier):
            raise RuntimeError("Please populate _deviceIdentity.unique_device_identifier before calling _createAlarmLimitObjectiveInstance")
        
        holder: InstanceHolder[ice_LocalAlarmLimitObjective] = InstanceHolder()
        holder.data = ice_LocalAlarmLimitObjective()
        holder.data.unique_device_identifier = self._deviceIdentity.unique_device_identifier
        holder.data.metric_id = metric_id
        holder.data.limit_type = limit_type

        holder.handle = self._alarmLimitObjectiveDataWriter.register_instance(holder.data)
        if holder.handle.is_nil:
            self.__log.warning(f"Unable to register instance: {holder.data}")
            holder.handle = None
        else:
            self.__registeredAlarmLimitObjectiveInstances.append(holder)

        return holder


    def _createSampleArrayInstance(self, metric_id: str, vendor_metric_id: str, instance_id: int, unit_id: str, frequency: int) -> InstanceHolder[ice_SampleArray]:
        """
        Creates an initial `SampleArray` instance and registers it on DDS

        :param metric_id: The metric_id of the new `SampleArray` instance
        :param vendor_metric_id: The vendor_metric_id of the new `SampleArray` instance
        :param instance_id: The instance_id of the new `SampleArray` instance
        :param unit_id: The unit_id of the new `SampleArray` instance
        :param frequency: The frequency of the new `SampleArray` instance
        :returns holder: The `InstanceHolder` of the `SampleArray` instance
        :raises RuntimeError: When `_deviceIdentity.unique_device_identifier` is not populated before calling
        """

        if (not self._deviceIdentity or not self._deviceIdentity.unique_device_identifier):
            raise RuntimeError("Please populate _deviceIdentity.unique_device_identifier before calling _createSampleArrayInstance")
        
        holder: InstanceHolder[ice_SampleArray] = InstanceHolder()
        holder.data = ice_SampleArray()
        holder.data.unique_device_identifier = self._deviceIdentity.unique_device_identifier
        holder.data.metric_id = metric_id
        holder.data.vendor_metric_id = vendor_metric_id
        holder.data.instance_id = instance_id
        holder.data.unit_id = unit_id
        holder.data.frequency = frequency

        holder.handle = self._sampleArrayDataWriter.register_instance(holder.data)
        if holder.handle.is_nil():
            self.__log.warning(f"Unable to register instance: {holder.data}")
            holder.handle = None
        else:
            self.__registeredSampleArrayInstances.append(holder)
        
        return holder


    def _unregisterAllInstances(self) -> None:
        """
        Unregisters all `Numeric`, `SampleArray`, `AlarmLimit`, `AlarmLimitObjective`, Patient `Alert`, and Technical `Alert` instances from DDS
        """

        self._unregisterAllNumericInstances()
        self._unregisterAllSampleArrayInstances()
        self._unregisterAllAlarmLimitInstances()
        self._unregisterAllAlarmLimitObjectiveInstances()
        self._unregisterAllPatientAlertInstances()
        self._unregisterAllTechnicalAlertInstances()


    def __unregisterAllAlertInstances(self, old: set[str], map: dict[str, InstanceHolder[ice_Alert]], writer: ice_DataWriter[ice_Alert]) -> None:
        """
        Private helper function that unregisters all alerts in the provided map of Alert instances from DDS

        :param old: A set of keys mapping to old `Alert` instances
        :param map: A map of currently active alert instances that need to be unregistered
        :param writer: The `Alert` `ice_DataWriter` that will unregister the Alert instances
        """

        for key in map.keys():
            self.__writeAlert(old, map, writer, key, None)


    def _unregisterAllPatientAlertInstances(self) -> None:
        """
        Unregisters all patient `Alert` instances from DDS
        """

        self.__unregisterAllAlertInstances(self.__oldPatientAlertInstances, self.__patientAlertInstances, self._patientAlertWriter)


    def _unregisterAllTechnicalAlertInstances(self) -> None:
        """
        Unregisters all technical `Alert` instances from DDS
        """

        self.__unregisterAllAlertInstances(self.__oldTechnicalAlertInstances, self.__technicalAlertInstances, self._technicalAlertWriter)


    def _unregisterAllAlarmLimitObjectiveInstances(self) -> None:
        """
        Unregisters all `AlarmLimitObjective` instances from DDS
        """

        while self.__registeredAlarmLimitObjectiveInstances:
            self._unregisterAlarmLimitObjectiveInstance(self.__registeredAlarmLimitObjectiveInstances[0])

    
    def _unregisterAllAlarmLimitInstances(self) -> None:
        """
        Unregisters all `AlarmLimit` instances from DDS
        """

        while self.__registeredAlarmLimitInstances:
            self._unregisterAlarmLimitInstance(self.__registeredAlarmLimitInstances[0])


    def _unregisterAllNumericInstances(self) -> None:
        """
        Unregisters all `Numeric` instances from DDS
        """

        while self.__registeredNumericInstances:
            self._unregisterNumericInstance(self.__registeredNumericInstances[0])


    def _unregisterAllSampleArrayInstances(self) -> None:
        """
        Unregisters all `SampleArray` instances from DDS
        """

        while self.__registeredSampleArrayInstances:
            self._unregisterSampleArrayInstance(self.__registeredSampleArrayInstances[0])

    
    def _unregisterNumericInstance(self, holder: InstanceHolder[ice_Numeric]) -> None:
        """
        Unregisters a single `Numeric` instance from DDS

        :param holder: The `InstanceHolder` instance holding the `Numeric` instance to unregister
        """

        if holder:
            self.__registeredNumericInstances.remove(holder)
            self._numericDataWriter.unregister_instance(holder.handle)


    def _unregisterSampleArrayInstance(self, holder: InstanceHolder[ice_SampleArray]) -> None:
        """
        Unregisters a single `SampleArray` instance from DDS

        :param holder: The `InstanceHolder` instance holding the `SampleArray` instance to unregister
        """

        if holder:
            self.__registeredSampleArrayInstances.remove(holder)
            self._sampleArrayDataWriter.unregister_instance(holder.handle)


    def _unregisterAlarmLimitInstance(self, holder: InstanceHolder[ice_AlarmLimit]) -> None:
        """
        Unregisters a single `AlarmLimit` instance from DDS

        :param holder: The `InstanceHolder` instance holding the `AlarmLimit` instance to unregister
        """

        if holder:
            self.__registeredAlarmLimitInstances.remove(holder)
            self._alarmLimitDataWriter.unregister_instance(holder.handle)


    def _unregisterAlarmLimitObjectiveInstance(self, holder: InstanceHolder[ice_LocalAlarmLimitObjective]) -> None:
        """
        Unregisters a single `LocalAlarmLimitObjective` instance from DDS

        :param holder: The `InstanceHolder` instance holding the `LocalAlarmLimitObjective` instance to unregister
        """

        if holder:
            self.__registeredAlarmLimitObjectiveInstances.remove(holder)
            self._alarmLimitObjectiveDataWriter.unregister_instance(holder.handle)


    def _numericSample(self, holder: Optional[InstanceHolder[ice_Numeric]], new_value: float | int, time: DeviceClock.Reading, metric_id: str = None, vendor_metric_id: str = None, unit_id: str = units.rosetta_MDC_DIM_DIMLESS, instance_id: int = 0) -> Optional[InstanceHolder[ice_Numeric]]:
        """
        Handles publishing and averaging of a `Numeric` to DDS.
        
        If only holder, new_value, and time are provided, then the provided holder will be used to publish the `Numeric`.
        If metric_id and vendor_metric_id are provided as well, then function will handle unregistering of the old `InstanceHolder`,
        registers a new one, and publishes using the new `InstanceHolder`. This new `InstanceHolder` will then be returned.
        If no holder is provided, then you must provide all other parameters so that a new one an be registered. This will also be returned in this case.
        If no new holder is created, then nothing will be returned

        :param holder: The `InstanceHolder` that holds the `Numeric` instance used for publishing to DDS
        :param new_value: The new value that you want to publish and average out
        :param time: A `DeviceClock.Reading` instance with the desired timestamp for the `Numeric` reading
        :param metric_id: The metric_id of the `Numeric` to publish (if different to that stored in in the provided `InstanceHolder`)
        :param vendor_metric_id: The vendor_metric_id of the `Numeric` to publish (if different to that stored in the provided `InstanceHolder`)
        :param unit_id: The unit_id of of the `Numeric` to publish (if different ot that stored in the provided `InstanceHolder`)
        :param instance_id: The instance_id of of the `Numeric` to publish (if different ot that stored in the provided `InstanceHolder`)
        :returns holder: The newly registered `InstanceHolder` if a new one was required to be created
        :raises ValueError: If a holder is not provided when metric_id and vendor_metric_id are also not provided
        """

        if new_value != None:
            # new_value always cast to float if not None
            new_value = float(new_value)

        if not metric_id or not vendor_metric_id:
            # Invoked if new instance is not needed/none of the parameters of the Numeric have changed
            if holder == None:
                raise ValueError("Must provide a holder if metric_id and vendor_metric_id not provided")
    
            holder.data.value = new_value
            if time.has_device_time():
                t: Time = DomainClock.to_DDS_time(time.get_device_time())
                holder.data.device_time.sec = t.sec
                holder.data.device_time.nanosec = t.nanosec
            else:
                holder.data.device_time.sec = 0
                holder.data.device_time.nanosec = 0

            t: Time = DomainClock.to_DDS_time(time.get_time())
            holder.data.presentation_time.sec = t.sec
            holder.data.presentation_time.nanosec = t.nanosec

            self._numericDataWriter.write(holder.data, holder.handle)

            if holder.data.metric_id in self.__averagesByNumeric:
                self.__averagesByNumeric[holder.data.metric_id].add(new_value)
            else:
                averager = Averager()
                averager.add(new_value)
                self.__averagesByNumeric[holder.data.metric_id] = averager

        else:
            # Invoked if metric_id and vendor_metric_id provided (handling registering new instance if required)
            if holder != None and (
                not holder.data.metric_id == metric_id or
                not holder.data.vendor_metric_id == vendor_metric_id or
                holder.data.instance_id != instance_id or
                not holder.data.unit_id == unit_id):

                self._unregisterNumericInstance(holder)
                holder = None

            if new_value != None:
                # If no holder, register a new instance and then publish the sample
                if holder == None:
                    holder = self._createNumericInstance(metric_id, vendor_metric_id, instance_id, unit_id)
                
                self._numericSample(holder, new_value, time)

            else:
                # Unregister instance if no new value
                if holder != None:
                    self._unregisterNumericInstance(holder)
                    holder = None

            return holder


    def _alarmLimitSample(self, holder: Optional[InstanceHolder[ice_AlarmLimit]], unit_id: str, new_value: float, metric_id: str = None, limit_type: ice_LimitType = None) -> Optional[InstanceHolder[ice_AlarmLimit]]:
        """
        Handles publishing of an `AlarmLimit` to DDS.
        `metric_id` and `limit_type` only need to be populated if you dont have a `holder` to provide. In this
        case, a new holder will be created using the provided metric_id an limit_type

        :param holder: The `InstanceHolder` that holds the `AlarmLimit` instance used for publishing to DDS. Can only be `None` if metric_id and limit_type are provided
        :param unit_id: The unit_id of the new `AlarmLimit` to publish
        :param new_value: The new value to publish to DDS
        :param metric_id: The metric_id of the new `InstanceHolder` that will be created if one isn't provided
        :param limit_type: The limit_type of the new `InstanceHolder` that will be created if one isn't provided
        :returns holder: The `InstanceHolder` that is newly created (if one needs to be created, otherwise None)
        :raises ValueError: If a holder is not provided when metric_id and limit_type are also not provided
        """

        if not metric_id or not limit_type:
            # Invoked if new instance is not needed/none of the parameters of the AlarmLimit have changed
            if holder == None:
                raise ValueError("Must provide a holder if metric_id and limit_type not provided")
            
            if new_value != holder.data.value or unit_id != holder.data.unit_identifier:
                holder.data.value = new_value
                holder.data.unit_identifier = unit_id
                self._alarmLimitDataWriter.write(holder.data, holder.handle)

        else:
            # Invoked if metric_id and limit_type provided (handling registering new instance if required)
            if holder != None and (
                not holder.data.unique_device_identifier == self._deviceIdentity.unique_device_identifier or
                not holder.data.metric_id == metric_id or
                not holder.data.limit_type == limit_type):

                self._unregisterAlarmLimitInstance(holder)
                holder = None

            if new_value != None:
                if holder == None:
                    holder = self._createAlarmLimitInstance(metric_id, limit_type)

                self._alarmLimitSample(holder, unit_id, new_value)

            else:
                if holder != None:
                    self._unregisterAlarmLimitInstance(holder)
                    holder = None

        return holder


    def _alarmLimitObjectiveSample(self, holder: Optional[InstanceHolder[ice_LocalAlarmLimitObjective]], unit_id: str, new_value: float, metric_id: str = None, limit_type: ice_LimitType = None) -> Optional[InstanceHolder[ice_LocalAlarmLimitObjective]]:
        """
        Handles publishing of a `LocalAlarmLimitObjective` to DDS
        `metric_id` and `limit_type` only need to be populated if you dont have a `holder` to provide. In this
        case, a new holder will be created using the provided metric_id an limit_type

        :param holder: The `InstanceHolder` that holds the `LocalAlarmLimitObjective` instance used for publishing to DDS
        :param unit_id: The unit_id of the new `LocalAlarmLimitObjective` to publish
        :param new_value: The new value to publish to DDS
        :param metric_id: The metric_id of the new `InstanceHolder` that will be created if one isn't provided
        :param limit_type: The limit_type of the new `InstanceHolder` that will be created if one isn't provided
        :returns holder: The `InstanceHolder` that is newly created (if one needs to be created, otherwise None)
        :raises ValueError: If a holder is not provided when metric_id and limit_type are also not provided
        """

        if not metric_id or not limit_type:
            # Invoked if new instance is not needed/none of the parameters of the LocalAlarmLimitObjective have changed
            if holder == None:
                raise ValueError("Must provide a holder if metric_id and limit_type not provided")
            
            if new_value != holder.data.value or unit_id != holder.data.unit_identifier:
                holder.data.value = new_value
                holder.data.unit_identifier = unit_id
                self._alarmLimitObjectiveDataWriter.write(holder.data, holder.handle)

        else:
            # Invoked if metric_id and limit_type provided (handling registering new instance if required)
            if holder != None and (
                not holder.data.unique_device_identifier == self._deviceIdentity.unique_device_identifier or
                not holder.data.metric_id == metric_id or
                not holder.data.limit_type == limit_type):

                self._unregisterAlarmLimitObjectiveInstance(holder)
                holder = None

            if new_value != None:
                if holder == None:
                    holder = self._createAlarmLimitObjectiveInstance(metric_id, limit_type)

                self._alarmLimitObjectiveSample(holder, unit_id, new_value)

            else:
                if holder != None:
                    self._unregisterAlarmLimitObjectiveInstance(holder)
                    holder = None

        return holder


    def _writeDeviceAlert(self, alert_state: str) -> None:
        """
        Publishes a `DeviceAlert` to DDS.

        :param alert_state: The alert_state to publish to DDS
        :raises RuntimeError: If there is no deviceAlertCondition registered for the device.
        """

        alert_state == "" if not alert_state else alert_state
        if self._deviceAlertConditionInstance:
            if not alert_state == self._deviceAlertConditionInstance.data.alert_state:
                self._deviceAlertConditionInstance.data.alert_state = alert_state
                self._deviceAlertConditionDataWriter.write(self._deviceAlertConditionInstance.data, self._deviceAlertConditionInstance.handle)
            
        else:
            raise RuntimeError("No _deviceAlertCondition; have you called _writeDeviceIdentity?")
        

    def __writeAlert(self, old: set[str], map: dict[str, InstanceHolder[ice_Alert]], writer: ice_DataWriter[ice_Alert], key: str, value: str) -> None:
        """
        Publishes an alert to DDS. If no value is provided, then the `InstanceHolder` at 'key' in the map will be unregistered.

        :param old: A set of old keys
        :param map: A dictionary mapping string keys to `InstanceHolder` instances holding `Alert` instances
        :param writer: An `ice_DataWriter` instance for `Alert`s that will be used to write the alert
        :param key: The key in the `map` parameter that maps to the `InstanceHolder` of the `Alert` you wish to publish
        :param value: The value stored in the alert
        """
        
        alert: InstanceHolder[ice_Alert] = map.get(key)

        if value == None:
            if alert:
                writer.unregister_instance(alert.data, alert.handle)
                map.pop(key, None)
        
        else:
            if not alert:
                alert = InstanceHolder()
                alert.data = ice_Alert()
                alert.data.unique_device_identifier = self._deviceIdentity.unique_device_identifier
                alert.data.identifier = key
                alert.handle = writer.register_instance(alert.data)
                map[key] = alert

            old.remove(key)
            if value != alert.data.text:
                alert.data.text = value
                writer.write(alert.data, alert.handle)


    def _markOldPatientAlertInstances(self) -> None:
        """
        Marks all of the currently active patient `Alert` instances as old
        """

        self.__oldPatientAlertInstances.clear()
        self.__oldPatientAlertInstances.update(self.__patientAlertInstances.keys())

    
    def _markOldTechnicalAlertInstances(self) -> None:
        """
        Marks all of the currently active technical `Alert` instances as old
        """

        self.__oldTechnicalAlertInstances.clear()
        self.__oldTechnicalAlertInstances.update(self.__technicalAlertInstances.keys())


    def _clearOldPatientAlertInstances(self) -> None:
        """
        Clears all old patient `Alert` instances and unregisters them from DDS
        """

        for key in self.__oldPatientAlertInstances:
            self._writePatientAlert(key, None)


    def _clearOldTechnicalAlertInstances(self) -> None:
        """
        Clearls all old technical `Alert` instances and unregisters them from DDS
        """

        for key in self.__oldTechnicalAlertInstances:
            self._writeTechnicalAlert(key, None)


    def _writePatientAlert(self, key: str, value: str) -> None:
        """
        Publishes a patient `Alert` to DDS.

        :param key: The key of the patient `Alert` to publish
        :param value: The value of the patient `Alert` to publish
        """

        self.__writeAlert(self.__oldPatientAlertInstances, self.__patientAlertInstances, self._patientAlertWriter, key, value)


    def _writeTechnicalAlert(self, key: str, value: str) -> None:
        """
        Publishes a technical `Alert` to DDS.

        :param key: The key of the technical `Alert` to publish
        :param value: The value of the technical `Alert` to publish
        """

        self.__writeAlert(self.__oldTechnicalAlertInstances, self.__technicalAlertInstances, self._technicalAlertWriter, key, value)


    def _sampleArraySample(
        self,
        holder: InstanceHolder[ice_SampleArray],
        new_values: list[float | int] | Collection[float | int] | NullSaveContainer[float | int],
        timestamp: DeviceClock.Reading,
        metric_id: Optional[str] = None,
        vendor_metric_id: Optional[str] = None,
        instance_id: int = 0,
        unit_id: Optional[str] = None,
        frequency: Optional[int] = None,
        length: Optional[int] = None
    ) -> Optional[InstanceHolder[ice_SampleArray]]:
        """
        Publishes a new `SampleArray` sample to DDS.

        If only holder, new_values, and timestamp are provided, then a sample will be published using the provided `InstanceHolder`.
        If you don't have an `InstanceHolder` to provide, or need to update the parameters within the provided one, then all of:
        metric_id, vendor_metric_id, unit_id, and frequency must be provided so that a new instance can be registered. 
        In this case, the new `InstanceHolder` will be returned. Otherwise nothing is returned.

        :param holder: The `InstanceHolder` of a `SampleArray` that will be used for publishing
        :param new_values: The new values to publish in the `SampleArray`. Must be an array(list), any Python object that is a `Collection`, or already in a `NullSaveContainer` child.
        :param timestamp: The timestamp to be published with the `SampleArray`.
        :param metric_id: If the metric_id needs to be updated or no holder is being provided, this is the metric_id of the `SampleArray` being published.
        :param vendor_metric_id: If the vendor_metric_id needs to be updated or no holder is being provided, this is the vendor_metric_id of the `SampleArray` being published.
        :param instance_id: If the instance_id needs to be updated or no holder is being provided, this is the instance_id of the `SampleArray` being published.
        :param unit_id: If the unit_id needs to be updated or no holder is being provided, this is the unit_id of the `SampleArray` being published.
        :param frequency: If the frequency needs to be updated or no holder is being provided, this is the frequency of the `SampleArray` being published.
        :param length: Very optionally, the length of the array if you provide an array of new_values
        :returns holder: A new `InstanceHolder` instance if a new one was registered with DDS, otherwise None
        :raises TypeError: If new_values is not an array(list), Collection, or instance of a child of NullSaveContainer
        :raises ValueError: If some but not all of metric_id, vendor_metric_id, unit_id, and frequency are provided, but not all. Either provide none to use the existing `InstanceHandle`, or all for a new one
        """

        # Put all new_values into corresponding containers
        if not isinstance(new_values, NullSaveContainer):
            if isinstance(new_values, list):
                container = ArrayContainer(new_values, length)
            elif isinstance(new_values, Collection):
                container = CollectionContainer(new_values)
            else:
                raise TypeError(f"new_values must be either list, Collection, or child of NullSaveContainer, not: {type(new_values)}")
        else:
            container = new_values


        if not metric_id or not vendor_metric_id or not unit_id or not frequency:
            self.__fill(holder, new_values)
            self.__publish(holder, timestamp)

        elif metric_id and vendor_metric_id and unit_id and frequency:
            holder = self.__ensureHolderConsistency(holder, metric_id, vendor_metric_id, instance_id, unit_id, frequency)

            if not container.is_null():
                timestamp = timestamp.refine_resolution_for_frequency(frequency, container.size())
                if holder is None:
                    holder = self._createSampleArrayInstance(metric_id, vendor_metric_id, instance_id, unit_id, frequency)

                self._sampleArraySample(holder, new_values, timestamp)

            else:
                if holder is not None:
                    self._unregisterSampleArrayInstance(holder)
                    holder = None

            return holder

        else:
            raise ValueError("If you want to register a new instance, you must provide all of: metric_id, vendor_metric_id, unit_id and frequency")


    def __fill(self, holder: InstanceHolder[ice_SampleArray], new_values: NullSaveContainer[int | float]) -> None:
        """
        Fills up a `SampleArray` in the provided `InstanceHolder` with the new values.
        Warning: This overrides the current values stored in the `InstanceHolder`'s `SampleArray`

        :param holder: The `InstanceHolder` containing the `SampleArray` to fill up
        :param new_values: The new values to fill the array up with
        """

        holder.data.values.value.clear()

        if not new_values.is_null():
            for n in new_values:
                holder.data.values.value.append(float(n))


    def __publish(self, holder: InstanceHolder[ice_SampleArray], device_timestamp: DeviceClock.Reading) -> None:
        """
        Finally performs publishing of a `SampleArray`. This is only a helper method.
        USE _sampleArraySample AS A USER

        :param holder: The `InstanceHolder` containng the `SampleArray` to publish
        :param device_timestamp: The timestamp of the device to publish with the `SampleArray`
        """

        if device_timestamp.has_device_time():
            t: Time = DomainClock.to_DDS_time(device_timestamp.get_device_time())
            holder.data.device_time.sec = int(t.sec)
            holder.data.device_time.nanosec = int(t.sec)
        
        else:
            holder.data.device_time.sec = 0
            holder.data.device_time.nanosec = 0

        adjusted = device_timestamp.refine_resolution_for_frequency(holder.data.frequency, len(holder.data.values.value))
        DomainClock.to_DDS_time(adjusted.get_time(), holder.data.presentation_time)

        self._sampleArrayDataWriter.write(holder.data, holder.handle if holder.handle else InstanceHandle.nil())

        #TODO ADD SQL HANDLING


    def __ensureHolderConsistency(self, holder: InstanceHolder[ice_SampleArray], metric_id: str, vendor_metric_id: str, instance_id: int, unit_id: str, frequency: int) -> Optional[InstanceHolder[ice_SampleArray]]:
        """
        Ensures that the attributes in the provided `InstanceHolder`'s `SampleArray` match what they should be.
        If consistency is wrong, the holder will be unregistered.

        :param holder: The `InstanceHolder` to validate consistency of
        :param metric_id: The metric_id to validate with
        :param vendor_metric_id: The vendor_metric_id to validate with
        :param instance_id: The instance_id to validate with
        :param unit_id: The unit_id to validate with
        :param frequency: The frequency to validate with
        :returns holder: The validated holder instance or None if the holder was invalid
        """

        if holder and (not holder.data.metric_id == metric_id or
                       not holder.data.vendor_metric_id == vendor_metric_id or
                       not holder.data.instance_id == instance_id or
                       not holder.data.frequency == frequency or
                       not holder.data.unit_id == unit_id):
            
            self._unregisterSampleArrayInstance(holder)
            holder = None

        return holder
    

    def iconResourceName() -> str:
        """
        Gets the icon resource name
        
        :returns name: The icon resource name
        """

        return None
