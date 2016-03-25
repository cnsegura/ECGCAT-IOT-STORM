package iot;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class BooleanFilter extends BaseFilter {
	public boolean isKeep(TridentTuple tuple) {
		//for debug
		//System.out.println("in boolFilter");
		return tuple.getBoolean(0);
	}

}
