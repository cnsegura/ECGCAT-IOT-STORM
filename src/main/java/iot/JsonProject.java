package iot;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.json.simple.JSONValue;

public class JsonProject extends BaseFunction {
	private Fields fields;
	
	public JsonProject(Fields fields) {
		this.fields = fields;
	}
	
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
		String json = tuple.getString(0);
		Map<String, Object> map = (Map<String, Object>)JSONValue.parse(json);
		Values values = new Values();
		for (int i = 0; i < this.fields.size(); i++) {
			values.add(map.get(this.fields.get(i)));
		}
		//for debug
		//System.out.println(values);
		
		collector.emit(values);
	}

}
