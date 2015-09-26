
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import javax.swing.plaf.synth.SynthSeparatorUI;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private String inFile;
  private BufferedReader br;
  
  
  public FileReaderSpout(String inFile) {
	this.inFile = inFile;
}

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

	   System.out.println("Loading " + inFile);
	   
		File file = new File(this.inFile);
		try {
			this.br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			System.out.println("File not found: " + inFile);
		}

    this.context = context;
    this._collector = collector;
  }

	@Override
	public void nextTuple() {

		try {
			String line = br.readLine();

			if (line != null) {
				_collector.emit(new Values(line));
			}

			else {
				Thread.sleep(5000);

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error emitting words");
		} catch (InterruptedException e) {
			System.out.println("INFO: Output thread was interrupted");
		}

	}

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {

	  try {
		  if (br != null) 
		  {
			  br.close();
		  }
	  } catch (IOException e) {
		  System.out.println("Error: Couldn't close file");
	  }
  }

  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
