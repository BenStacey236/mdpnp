from ice import ice_InfusionStatus
from ice import ice_InfusionStatusTopic
from ice import ice_InfusionObjective
from ice import ice_InfusionObjectiveTopic

from AbstractSimulatedConnectedDevice import AbstractSimulatedConnectedDevice
from SimulatedInfusionPump import SimulatedInfusionPump
from ice_DataWriter import ice_DataWriter
from ice_DataReader import ice_DataReader
from EventLoop import EventLoop, ConditionHandler

from rti.connextdds import InstanceHandle
from rti.connextdds import StringSeq
from rti.connextdds import Query
from rti.connextdds import QueryCondition
from rti.connextdds import DataState
from rti.connextdds import SampleState
from rti.connextdds import ViewState
from rti.connextdds import InstanceState
import rti.connextdds as dds

from overrides import overrides


class Pump(SimulatedInfusionPump):
    """
    A specialised `SimulatedInfusionPump` with _receivePumpStatus implimented
    """

    def __init__(self, device: 'SimInfusionPump'):
        """
        Initialises a new `Pump` instance.

        :param device: A 'SimInfusionPump` instance. Allows access to internal methods for updating pump status
        """

        super().__init__()
        self.__device = device


    @overrides
    def _receivePumpStatus(self, drug_name, infustion_active, drug_mass_mcg, solution_volume_ml, volume_to_be_infused_ml, infusion_duration_seconds, infusion_fraction_complete):
        
        self.__device._infusionStatus.drug_name = drug_name
        self.__device._infusionStatus.infusionActive = infustion_active
        self.__device._infusionStatus.drug_mass_mcg = drug_mass_mcg
        self.__device._infusionStatus.solution_volume_ml = solution_volume_ml
        self.__device._infusionStatus.volume_to_be_infused_ml = volume_to_be_infused_ml
        self.__device._infusionStatus.infusion_duration_seconds = infusion_duration_seconds
        self.__device._infusionStatus.infusion_fraction_complete = infusion_fraction_complete
        self.__device._infusionStatusWriter.write(self.__device._infusionStatus, self.__device._infusionStatusHandle)


class PumpConditionHandler(ConditionHandler):
    """
    A Specialised condition handler for `SimInfusionPump`
    """

    def __init__(self, device: 'SimInfusionPump'):
        """
        Initialises a new `PumpConditionHandler` instance
        
        :param device: A `SimInfusionPump instance. Gives access to internal methods
        """

        self.__device = device
        super().__init__()


    @overrides
    def conditionChanged(self, condition):
        
        while True:
            samples = self.__device.__infusionObjectiveReader.read_w_condition(condition)
            if not samples:
                break

            for sample in samples:
                si = sample.info()
                
                if not si.valid:
                    continue

                data = sample.data()
                self.__device._stopThePump(data.stopInfusion)


class SimInfusionPump(AbstractSimulatedConnectedDevice):
    """
    A simulated infusion pump for testing with OpenICE supervisor
    """

    def __init__(self, subscriber, publisher, event_loop):
        super().__init__(subscriber, publisher, event_loop)
        self._writeIdentity()

        self.__pump = Pump(self)
        self._infusionStatus: ice_InfusionStatus = ice_InfusionStatus()
        self._infusionStatusWriter: ice_DataWriter[ice_InfusionStatus] = ice_DataWriter(self._domainParticipant, ice_InfusionStatusTopic, ice_InfusionStatus)

        self._infusionStatus.unique_device_identifier = self._deviceIdentity.unique_device_identifier
        self._infusionStatusHandle: InstanceHandle[ice_InfusionStatus] = self._infusionStatusWriter.register_instance(self._infusionStatus)

        self._infusionStatus.drug_name = "Morphine"
        self._infusionStatus.drug_mass_mcg = 20
        self._infusionStatus.solution_volume_ml = 120
        self._infusionStatus.infusion_duration_seconds = 3600
        self._infusionStatus.infusion_fraction_complete = 0.0
        self._infusionStatus.infusionActive = True
        self._infusionStatus.volume_to_be_infused_ml = 100

        self._infusionStatusWriter.write(self._infusionStatus, self._infusionStatusHandle)

        self.__infusionObjectiveReader: ice_DataReader[ice_InfusionObjective] = ice_DataReader(self._domainParticipant, ice_InfusionObjectiveTopic, ice_InfusionObjective)

        self.__infusionObjectiveQueryCondition: QueryCondition = QueryCondition(Query(self.__infusionObjectiveReader.reader, f"unique_device_identifier = '{self._deviceIdentity.unique_device_identifier}'"), DataState(SampleState.NOT_READ, ViewState.ANY, InstanceState.ALIVE))

        self.eventLoop.addHandler(self.__infusionObjectiveQueryCondition, PumpConditionHandler(self))


    @overrides
    def connect(self, device_name) -> bool:
        self.__pump.connect()
        return super().connect(device_name)
    

    @overrides
    def disconnect(self):
        self.__pump.disconnect()
        return super().disconnect()
    

    def _writeIdentity(self) -> None:
        """
        Writes the device identity to DDS including this specific model type
        """

        self._deviceIdentity.model = "Infusion Pump (Simulated)"
        self._writeDeviceIdentity()


    def _stopThePump(self, stopThePump: bool) -> None:
        """
        Stops the pump if True provided, otherwise it doesn't stop
        
        :param stopThePump: Whether or not to stop the pump. True to stop, False not to stop.
        """

        self.__pump.setInterlockStop(stopThePump)


    @overrides
    def shutdown(self):

        self.eventLoop.removeHandler(self.__infusionObjectiveQueryCondition)
        self.__infusionObjectiveQueryCondition = None

        self.__infusionObjectiveReader = None
        self._infusionStatusHandle = None
        self._infusionStatusWriter = None

        return super().shutdown()


    @overrides
    def _getIconPath(self) -> str:
        return "interop-lab/demo-devices/src/main/resources/org/mdpnp/devices/simulation/pump/pump.png"


if __name__ == "__main__":

    qos_provider = dds.QosProvider("python-driver-kit/USER_QOS_PROFILES.xml")
    particpant_qos = qos_provider.participant_qos_from_profile("ice_Library::ice_Profile")
    particpant_qos.resource_limits.type_code_max_serialized_length = 512 # AGAIN TEMP QOS FIX
    sub_qos = qos_provider.subscriber_qos_from_profile("ice_Library::ice_Profile")
    pub_qos = qos_provider.publisher_qos_from_profile("ice_Library::ice_Profile")

    participant = dds.DomainParticipant(0, particpant_qos)
    subscriber = dds.Subscriber(participant, sub_qos)
    publisher = dds.Publisher(participant, pub_qos)
    eventLoop = EventLoop()

    pump = SimInfusionPump(subscriber, publisher, eventLoop)
    pump.connect("Infusion Pump (Simulated)")