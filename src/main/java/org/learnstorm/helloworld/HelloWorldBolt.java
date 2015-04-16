package org.learnstorm.helloworld;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class HelloWorldBolt extends BaseRichBolt {

	private static final long serialVersionUID = 641152530527935569L;
	public static Logger LOG = Logger.getLogger(HelloWorldBolt.class);
	private int myCount = 0;

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
	}
	
			@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String test = input.getStringByField("sentence");
		if ( test.equals("Hello World") ) {
			myCount ++;
			System.out.println("Found a Hello World!");
			LOG.debug("Found a Hello World");
		}
		else {
			System.out.println("Found a not Hello World!");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("myCount"));
	}

}
