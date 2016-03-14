package iot;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.microsoft.windowsazure.services.servicebus.*;
import com.microsoft.windowsazure.services.servicebus.models.*;

import backtype.storm.topology.OutputFieldsDeclarer;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.*;
import com.microsoft.windowsazure.exception.ServiceException;

import javax.xml.datatype.*;

//This class sends a message to Azure Service Bus.
public class ServiceBusNotification extends BaseFunction {
	
	Configuration config = ServiceBusConfiguration.configureWithSASAuthentication("ecgcat-iot-servicebus", "RootManageSharedAccessKey", "WMOWbISHCy+EkplmooMGQSUuo0ONa/ALPXh6jhSWFto=", ".servicebus.windows.net");
	ServiceBusContract service = ServiceBusService.create(config);
	TopicInfo topicInfo = new TopicInfo("TempData");
	
	@Override
	public void execute(TridentTuple tple, TridentCollector collector) {
		try {
			GetTopicResult getTopic = service.getTopic("TempData");
			BrokeredMessage message = new BrokeredMessage("Temperature Out of Range");
			service.sendTopicMessage("TempData", message);
		} catch (ServiceException e) {
			try {
				CreateTopicResult result = service.createTopic(topicInfo);
				BrokeredMessage message = new BrokeredMessage("Temperature Out of Range");
				service.sendTopicMessage("TempData", message);
			} catch (ServiceException e1) {
				System.out.print("Service Unavailable");
				System.exit(-1);
			}
			
		}
		
	}
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	// this bolt does not emit anything
    }
}
