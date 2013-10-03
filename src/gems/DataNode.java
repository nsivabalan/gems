package gems;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import gems.MetaDataProtos.MetaData;
import gems.MetaDataProtos.MetaData.GossipEntry;
import gems.MetaDataProtos.MetaData.SuspectEntry;
import gems.MetaDataProtos.MetaData.SuspectEntry.SuspectRowEntry;

import org.zeromq.ZMQ;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * User: sivabalan
 * Date: 09/01/13
 * Description:
 * 		Node representing the server 
 */
public class DataNode implements Runnable{
	public ZMQ.Context context;
	private int id;
	public Commons.Protocol protocol;
	private int  port;
	public String inetAddr;
	private HashMap<Integer, Integer> liveList = null;
	private int[] gossipList = null;
	private int[] ports;
	private int[][] suspectMatrix = null;
	ZMQ.Socket publisher = null;
	private int total ;
	private ArrayList<ZMQ.Socket> zmqSockets = null;
	protected final static Logger log = Logger.getLogger(DataNode.class.getName());

	public DataNode(ZMQ.Context context, Commons.Protocol protocol, String inetAddr, int port, int id, int total, int[] ports) {
		this.context = context;
		this.inetAddr = inetAddr;
		this.protocol = protocol;
		this.id = id;
		this.port = port;
		this.ports = ports;
		this.total = total;
		liveList = new HashMap<Integer, Integer>();
		gossipList = new int[total];
		suspectMatrix = new int[total][total];
		zmqSockets = new ArrayList<ZMQ.Socket>();
		context = ZMQ.context(1);
		publisher = context.socket(ZMQ.PUB);

	}

	public void init(int[] ports)
	{
		for(int i=0;i<total;i++){
			gossipList[i] = 0; 
			liveList.put(i, 1);
			for(int k=0;k<total;k++)
				suspectMatrix[i][k] = 0;
		}

		StringBuffer buffer = new StringBuffer();
		buffer.append("Gossip List \n");
		for(int i=0;i<total;i++)
			buffer.append(" "+i+" "+gossipList[i]);
		buffer.append("\n");
		buffer.append("Live List");
		for(int i=0;i<total;i++)
			buffer.append(" "+liveList.get(i));
		buffer.append("\n");
		buffer.append("Suspect Matrix");
		for(int i=0;i<total;i++){
			for(int k=0;k<total;k++)
				buffer.append(" "+suspectMatrix[i][k]);
			buffer.append("\n");
		}
		buffer.append("\n");
		log.info(buffer.toString());

		publisher.bind(Commons.makeFullAddr(protocol, inetAddr, port));
	}


	private void updateInputdata(MetaData input)
	{
		int source = input.getId();
		int[][] inputMatrix = new int[total][total];
		List<GossipEntry> gossipEntries = input.getGossipListList();
		HashMap<Integer, Integer> gossipMap = new HashMap<Integer, Integer>();
		for(GossipEntry entry : gossipEntries)
			gossipMap.put(entry.getId(), entry.getHeartbeat());

		List<SuspectEntry> suspectEntries = input.getSuspectMatrixList();
		for(SuspectEntry row: suspectEntries)
		{
			int rowIndex = row.getRowIndex();
			List<SuspectRowEntry> colEntries = row.getSuspectRowList();
			for(SuspectRowEntry colEntry: colEntries)
				inputMatrix[rowIndex][colEntry.getColindex()] = colEntry.getValue();
		}

		updateMetaData(source, gossipMap, inputMatrix);
	}


	private void updateMetaData(int source, HashMap<Integer, Integer> inputList, int[][] inputMatrix)
	{
		gossipList[source] = 0;
		if(suspectMatrix[id][source] == 1)
		{
			suspectMatrix[id][source] = 0;
			if(liveList.get(source) == 0) liveList.put(source, 1);
		}
		log.info("Gossip count for "+source+" = "+gossipList[source]);

		for(int i =0;i<total;i++)
		{
			if(i != id)
			{
				if(gossipList[i] >= inputList.get(i))
				{
					if(inputList.get(i) < Commons.GOSSIP_TH){
						suspectMatrix[id][i] = 0;
						liveList.put(i, 1);
					}

					for(int j=0;j<total;j++)
					{
						gossipList[i] = inputList.get(i);
						suspectMatrix[i][j] = inputMatrix[i][j];
					}
				}
			}
		}

		for(int i=0;i<total;i++)
		{
			suspectMatrix[source][i] = inputMatrix[source][i];
		}

		this.updateLiveness();

		this.printMetaData();

	}

	private void updateLiveness()
	{
		for(int i =0;i<total; i++)
		{
			if(i != id){
				int count = 0;
				for(int j =0;j<total; j++)
				{
					if(i != j)
						if(suspectMatrix[j][i] == 1)
							count++;
				}
				if(count == total -1)
					liveList.put(i, 0);
			}
		}
	}

	@Override
	public void run() {
		log.info("Subscribing to neighbours exchanges... ");
		for(int port: ports){
			SubscribeThread thrd = new SubscribeThread(context, protocol, inetAddr, port);
			new Thread(thrd).start();
		}

		while(true)
		{
			try {
				Thread.sleep(Commons.GOSSIP_INTERVAL_MS);
				sendMetaData();
				updateGossipCount();
				updateSuspectMatrix();
				updateLiveness();
				log.info("--------------------- Updated Meta Data -----------------------");
				printMetaData();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	private void updateSuspectMatrix()
	{
		for(int i =0;i<total;i++)
		{
			if(i != id)
			{
				if(gossipList[i] >= Commons.GOSSIP_TH)
				{
					suspectMatrix[id][i] = 1;
				}
			}
		}
	}

	public void printInputMetaData(MetaData report)
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append("Source "+report.getId()+"\n");
		List<GossipEntry> gossipList = report.getGossipListList();
		buffer.append("Gossip List \n");
		int gossipcount = report.getGossipListCount();

		for(int i=0;i<gossipcount;i++)
		{
			buffer.append(" "+gossipList.get(i).getId()+" "+gossipList.get(i).getHeartbeat()+"\n");
		}
		buffer.append("\n \n");
		buffer.append("Suspect Matrix \n");
		int suspectRowCount = report.getSuspectMatrixCount();
		List<SuspectEntry> suspectEntries = report.getSuspectMatrixList();
		for(int i =0;i<suspectRowCount;i++)
		{
			int rowLength  = suspectEntries.get(i).getSuspectRowCount();
			List<SuspectRowEntry> rowEntry= suspectEntries.get(i).getSuspectRowList();
			buffer.append(i+"\t");
			for(int j=0;j<rowLength;j++)
			{
				buffer.append(" "+rowEntry.get(j).getValue());
			}
			buffer.append("\n");

		}
		log.info(buffer.toString());
		this.updateInputdata(report);

	}

	private void printMetaData()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append("\n ------------ Printing MetaData -------- \n");
		buffer.append("Source "+id+"\n");
		buffer.append("Gossip List \n");


		for(int i=0;i<total;i++)
		{
			buffer.append(i+" "+gossipList[i]+"\n");
		}
		buffer.append("\n");
		buffer.append("Suspect Matrix \n");
		for(int i =0;i<total;i++)
		{
			buffer.append(i+"\t");
			for(int j=0;j<total;j++)
			{
				buffer.append(" "+suspectMatrix[i][j]);
			}
			buffer.append("\n");
		}

		buffer.append("\nLive List \n");
		for(int i : liveList.keySet())
		{
			buffer.append(" "+i+"-"+liveList.get(i)+",");
		}
		buffer.append("\n ------------ End of Print ---------------- \n");
		log.info(buffer.toString());
	}

	public void listMenu() throws NumberFormatException, IOException, InterruptedException
	{
		System.out.println("Enter your option : 1. List metadata\t2. Send Msgs\t3. Exit\n ");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		int option = Integer.parseInt(br.readLine());
		while(option != 4)
		{
			switch(option){
			case 1:
				printMetaData();
				break;
			case 2:
				sendMetaData();
				break;
			case 3:
				updateGossipCount();
			}
			String line = br.readLine();
			while(!line.equalsIgnoreCase("next"))
				line = br.readLine();

			System.out.println("Enter your option : 1. List metadata\t2. Send Msgs\t3. Exit\n ");
			option = Integer.parseInt(br.readLine());
		}


		publisher.close();
		context.term ();
	}


	private void updateGossipCount()
	{
		for(int i =0;i<total;i++){
			if(i != id){
				gossipList[i]++;
				if(gossipList[i] >= Commons.GOSSIP_TH)
					suspectMatrix[id][i] = 1;
			}
		}
	}

	private void sendMetaData() throws InterruptedException
	{

		MetaData report = getMetaData();
		publisher.send(report.toByteArray(), 0);
		Thread.sleep(5000);

	}


	public MetaData getMetaData()
	{
		MetaData.Builder builder = MetaData.newBuilder();

		builder.setId(id);
		MetaData.GossipEntry.Builder gossipBuilder = null;

		for(int i = 0;i<total; i++){
			gossipBuilder = MetaData.GossipEntry.newBuilder();
			gossipBuilder.setId(i);
			gossipBuilder.setHeartbeat(gossipList[i]);
			builder.addGossipList(gossipBuilder.build());
		}

		MetaData.SuspectEntry.Builder suspectEntryBuilder = null;

		for(int i = 0;i<total; i++){
			suspectEntryBuilder = MetaData.SuspectEntry.newBuilder();
			MetaData.SuspectEntry.SuspectRowEntry.Builder suspectMatrixRowEntryBuilder = null;
			suspectEntryBuilder.setRowIndex(i);
			for(int j=0;j<total;j++)
			{
				suspectMatrixRowEntryBuilder = MetaData.SuspectEntry.SuspectRowEntry.newBuilder();
				suspectMatrixRowEntryBuilder.setColindex(j);
				suspectMatrixRowEntryBuilder.setValue(suspectMatrix[i][j]);
				suspectEntryBuilder.addSuspectRow(suspectMatrixRowEntryBuilder.build());
			}

			builder.addSuspectMatrix(suspectEntryBuilder.build());
		}

		return builder.build();

	}
	
	
	/*private void setConnections(int[] ports)
	{
		for(int port: ports)
		{
			ZMQ.Socket socket = context.socket(ZMQ.PUSH);
			socket.connect(Commons.makeFullAddr(protocol, inetAddr, port));
			socket.setLinger(0);
			zmqSockets.add(socket);
			System.out.println("Setting connection to "+port);
		}
	}*/


	class SubscribeThread implements Runnable{
		public ZMQ.Context context;
		public Commons.Protocol protocol;
		public int port;
		public String inetAddr;
		public SubscribeThread(ZMQ.Context context, Commons.Protocol protocol, String inetAddr, int port) {
			this.context = context;
			this.inetAddr = inetAddr;
			this.port = port;
			this.protocol = protocol;
		}

		@Override
		public void run() {

			ZMQ.Context context = ZMQ.context(1);

			ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
			subscriber.connect(Commons.makeFullAddr(protocol, inetAddr, port));

			subscriber.subscribe("".getBytes());

			//	log.info("Listening on "+port);
			while(!Thread.interrupted()) {
				byte[] msg = subscriber.recv();
				try {
					MetaData report = MetaData.parseFrom(msg);
					printInputMetaData(report);

				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
				}
			}
			subscriber.close();
			context.term();
		}
	}

	public static void main(String args[]) throws NumberFormatException, IOException, InterruptedException
	{
		int length = args.length;
		if(length <= 3){
			throw new IllegalArgumentException("Usage : DataNode [id] [Total no of nodes] [port] [List of hostnames and ports to listen to] ");
		}
		
		int id = Integer.parseInt(args[0]);
		int total = Integer.parseInt(args[1]);
		int port = Integer.parseInt(args[2]);
		int[] ports = new int[length -3];
		for(int i = 3;i<length;i++){
			ports[i-3] = Integer.parseInt(args[i]);
		}
		
		StringBuffer buffer = new StringBuffer();
		for(int i=0;i<ports.length;i++)
			buffer.append(" "+ports[i]);
		log.info(buffer.toString());
		
		DataNode obj = new DataNode( ZMQ.context(1), Commons.Protocol.TCP,
				Commons.LOCAL_HOST, port, id, total, ports);
		obj.init(ports);
		new Thread(obj).start();
		obj.listMenu();

	}

}