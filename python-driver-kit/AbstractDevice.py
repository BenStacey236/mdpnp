from ice import ice_Alert
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

from abc import ABC
from typing import TypeVar, Final, Generic, Optional
import logging
import threading


T = TypeVar('T')
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
        

class AbstractDevice(ABC):
    """
    Python equivalent to the AbstractDevice java class. All other OpenICE devices 
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

        self._patientAlertWriter: Final[ice_DataWriter[ice_Alert]] = None

        self._technicalAlertWriter: Final[ice_DataWriter[ice_Alert]] = None

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
            #self.__writeAlert(old, map, writer, key, None)
            pass


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


    def _numericSample(self, holder: InstanceHolder[ice_Numeric], new_value: float, time: DeviceClock.Reading) -> None:
        """
        Handles publishing and averaging of a new `Numeric`.

        :param holder: The `InstanceHolder` that holds the `Numeric` instance used for publishing to DDS
        :param new_value: The new value that you want to publish and average out
        :param time: A `DeviceClock.Reading` instance with the desired timestamp for the `Numeric` reading
        """
        
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


    def _alarmLimitSample(self, holder: Optional[InstanceHolder[ice_AlarmLimit]], unit_id: str, new_value: float, metric_id: str = None, limit_type: ice_LimitType = None) -> Optional[InstanceHolder[ice_AlarmLimit]]:
        """
        Handles publishing of a new `AlarmLimit` to DDS.
        `metric_id` and `limit_type` only need to be populated if you dont have a `holder` to provide. In this
        case, a new holder will be created using the provided metric_id an limit_type

        :param holder: The `InstanceHolder` that holds the `AlarmLimit` instance used for publishing to DDS. Can only be `None` if metric_id and limit_type are provided
        :param unit_id: The unit_id of the new `AlarmLimit` to publish
        :param new_value: The new value to publish to DDS
        :param metric_id: The metric_id of the new `InstanceHolder` that will be created if one isn't provided
        :param limit_type: The limit_type of the new `InstanceHolder` that will be created if one isn't provided
        :returns holder: The `InstanceHolder` that is newly created (if one needs to be create, otherwise None)
        :raises ValueError: If a holder is not provided when metric_id and limit_type are also not provided
        """

        if not metric_id or not limit_type:
            if holder == None:
                raise ValueError("Must provide a holder if metric_id and limit_type not provided")
            
            if new_value != holder.data.value or unit_id != holder.data.unit_identifier:
                holder.data.value = new_value
                holder.data.unit_identifier = unit_id
                self._alarmLimitDataWriter.write(holder.data, holder.handle)

        else:
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
        Handles publishing of a new `LocalAlarmLimitObjective` to DDS

        :param holder: The `InstanceHolder` that holds the `LocalAlarmLimitObjective` instance used for publishing to DDS
        :param unit_id: The unit_id of the new `LocalAlarmLimitObjective` to publish
        :param new_value: The new value to publish to DDS
        """

        if not metric_id or not limit_type:
            if holder == None:
                raise ValueError("Must provide a holder if metric_id and limit_type not provided")
            
            if new_value != holder.data.value or unit_id != holder.data.unit_identifier:
                holder.data.value = new_value
                holder.data.unit_identifier = unit_id
                self._alarmLimitObjectiveDataWriter.write(holder.data, holder.handle)

        else:
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
