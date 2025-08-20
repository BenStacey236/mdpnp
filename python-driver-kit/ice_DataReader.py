import rti.connextdds as dds
from typing import Optional, TypeVar, Generic


T = TypeVar('T')
class ice_DataReader(Generic[T]):
    """
    A Generic class that handles reading of data from DDS. 
    
    Generic on a Type `T`, which is an ice object defined in the `ice.idl` file
    """

    def __init__(self, participant: dds.DomainParticipant, topic_name: str, data_type: T) -> None:
        """
        Initialises a new `ice_DataReader` instance

        :param participant: A DDS `DomainParticipant` object used to initialise the reader
        :param topic_name: The topic name of the data type being used
        :param data_type: An ice datatype class that the reader instance will be handling
        """

        self.topic = dds.Topic(participant, topic_name, data_type)
        self.reader = dds.DataReader(participant.implicit_subscriber, self.topic)


    def read(self):
        return self.reader.read()


    def get_key_value(self, handle: dds.InstanceHandle) -> T:
        """
        Gets the key value for a given instance handle.

        :param handle: `InstanceHandle` identifying the instance
        """

        return self.reader.key_value(handle)


    def lookup_instance(self, key_holder: T) -> dds.InstanceHandle:
        """
        Looks up an instance by key value.

        :param key_holder: Instance containing the key
        :return: InstanceHandle of the matching instance
        """

        return self.reader.lookup_instance(key_holder)
