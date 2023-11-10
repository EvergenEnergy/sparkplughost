package sparkplughost

type messageType string

const (
	messageTypeNBIRTH messageType = "NBIRTH"
	messageTypeNDEATH messageType = "NDEATH"
	messageTypeNDATA  messageType = "NDATA"
	messageTypeNCMD   messageType = "NCMD"
	messageTypeDBIRTH messageType = "DBIRTH"
	messageTypeDDEATH messageType = "DDEATH"
	messageTypeDDATA  messageType = "DDATA"
	messageTypeDCMD   messageType = "DCMD"
	messageTypeSTATE  messageType = "STATE"
)

func (m messageType) isDeviceMessage() bool {
	return m == messageTypeDBIRTH ||
		m == messageTypeDDEATH ||
		m == messageTypeDDATA ||
		m == messageTypeDCMD
}

func (m messageType) isEdgeNodeMessage() bool {
	return m == messageTypeNBIRTH ||
		m == messageTypeNDEATH ||
		m == messageTypeNDATA ||
		m == messageTypeNCMD
}
