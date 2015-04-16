package org.learnstorm.helloworld;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HelloWorldSpout extends BaseRichSpout {

	private static final long serialVersionUID = -3484409003918494839L;
	
	private static Logger LOG = Logger.getLogger(HelloWorldSpout.class);
	private SpoutOutputCollector collector;
	private int referenceNumber;
	private static final int MAX_NUMBER = 10;
	
	public HelloWorldSpout(){
		final Random random = new Random();
		referenceNumber = random.nextInt(MAX_NUMBER);
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Utils.sleep(100);
		final Random random = new Random();
		int instanceNumber = random.nextInt(MAX_NUMBER);
		
		if ( instanceNumber == referenceNumber ) {
			collector.emit(new Values("Hello World"));
		}
		else {
			collector.emit(new Values("Another message"));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sentence"));
	}

}
