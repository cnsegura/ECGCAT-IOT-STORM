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
    private double threshold;
    
    public ExtractTemp(double threshold) {
    	this.threshold = threshold;
    }
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	
    	
    	//code below for working on "offline" mode
    	//String strTemp = tuple.getString(0);
    	//float val = Float.parseFloat(strTemp);
    	
    	//code below for working on real data
    	double val = tuple.getDouble(0); //select the Temp from stream
    	
    	//evaluate state
    	State newState = val < this.threshold ? State.BELOW : State.ABOVE;
    	boolean stateChange = this.last != newState;
    	
    	//for debug
    	System.out.println(val);
    	
    	collector.emit(new Values(stateChange,threshold));
    }
    
}
