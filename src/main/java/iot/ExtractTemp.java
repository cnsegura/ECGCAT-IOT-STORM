package iot;


import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class ExtractTemp extends BaseFunction {
    
    private static enum State {
    	BELOW, ABOVE;
    }
    
    private State last = State.BELOW;
    private float threshold;
    
    public ExtractTemp(float threshold) {
    	this.threshold = threshold;
    }
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	float val = tuple.getFloat(1); //select the Temp from stream
    	State newState = val < this.threshold ? State.BELOW : State.ABOVE;
    	boolean stateChange = this.last != newState;
    	
    	//for debug
    	//System.out.println(value1);
    	
    	collector.emit(new Values(stateChange,threshold));
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	// this bolt does not emit anything
    }
}
