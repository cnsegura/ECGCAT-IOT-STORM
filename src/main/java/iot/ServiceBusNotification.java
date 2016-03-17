package iot;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.microsoft.windowsazure.services.servicebus.*;
import com.microsoft.windowsazure.services.servicebus.models.*;

import backtype.storm.topology.OutputFieldsDeclarer;

import com.microsoft.windowsazure.Configuration;
//import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.exception.ServiceException;

import java.util.Map;

//This class sends a message to Azure Service Bus.
public class ServiceBusNotification extends BaseFunction {
	private Configuration config;
	private ServiceBusContract service;
	private TopicInfo topicInfo;
	
	public void prepare(Map conf, TridentOperationContext context) {
		config = ServiceBusConfiguration.configureWithSASAuthentication("ecgcat-iot-servicebus", "RootManageSharedAccessKey", "INSERTKEYHERE", ".servicebus.windows.net");
		service = ServiceBusService.create(config);
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		//for debug
		System.out.println("in service bus");
		
		BrokeredMessage message = new BrokeredMessage("Temperature at or above 85F.");
		try {
			
			service.sendMessage("tempdata", message);
			//System.exit(0);
		} catch (ServiceException e) {
			System.out.println("send data bombed");
			//e.printStackTrace();
		}
		
	}
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	// this bolt does not emit anything
    }
}
