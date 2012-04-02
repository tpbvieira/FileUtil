package jnetpcap.jxta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import net.jxta.document.AdvertisementFactory;
import net.jxta.document.StructuredDocumentFactory;
import net.jxta.document.XMLDocument;
import net.jxta.endpoint.Message;
import net.jxta.endpoint.Message.ElementIterator;
import net.jxta.endpoint.MessageElement;
import net.jxta.impl.endpoint.router.EndpointRouterMessage;
import net.jxta.impl.util.pipe.reliable.Defs;
import net.jxta.parser.JxtaSocketFlow;
import net.jxta.protocol.PipeAdvertisement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Jxta;
import org.jnetpcap.protocol.tcpip.Jxta.JxtaMessageType;
import org.jnetpcap.protocol.tcpip.JxtaUtils;
import org.jnetpcap.protocol.tcpip.Tcp;

public class SocketStatisticsMapper extends Mapper<LongWritable, Text, Text, SortedMapWritable> {

	private static final String socketNamespace = "JXTASOC";
	private static final String reqPipeTag = "reqPipe";
	private static final String remPipeTag = "remPipe";	
	private static final String closeTag = "close";

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		final Context ctx = context;
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);	
		Path srcPath = new Path(hdfs.getWorkingDirectory() + "/" + value);	
		Path dstPath = new Path("/tmp/");		

		hdfs.copyToLocalFile(srcPath, dstPath);

		final StringBuilder errbuf = new StringBuilder();
		final Pcap pcap = Pcap.openOffline(dstPath.toString() + "/" +value, errbuf);
		if (pcap == null) {
			throw new InterruptedException("Impossible create PCAP file");
		}

		final SortedMap<Integer,JxtaSocketFlow> dataFlows = new TreeMap<Integer,JxtaSocketFlow>();
		final SortedMap<Integer,JxtaSocketFlow> ackFlows = new TreeMap<Integer,JxtaSocketFlow>();

		generateHalfSocketFlows(errbuf, pcap, dataFlows, ackFlows);
		printFlowStatistics(ctx,dataFlows,ackFlows);
	}

	private static void generateHalfSocketFlows(final StringBuilder errbuf, final Pcap pcap, final SortedMap<Integer,JxtaSocketFlow> dataFlows, final SortedMap<Integer,JxtaSocketFlow> ackFlows) {
		final SortedMap<Integer,Jxta> fragments = new TreeMap<Integer,Jxta>();		
		final SortedMap<Integer,Jxta> pipeReqst = new TreeMap<Integer,Jxta>(); 

		pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
			Tcp tcp = new Tcp();
			Jxta jxta = new Jxta();

			public void nextPacket(JPacket packet, StringBuilder errbuf) {				

				if(packet.hasHeader(Tcp.ID)){
					packet.getHeader(tcp);
										
					// Looking for tcp fragmentation 
					if(fragments.size() > 0 && tcp.getPayloadLength() > 0){
						Ip4 ip = new Ip4();
						packet.getHeader(ip);
						int tcpFlowId = JxtaUtils.getFlowId(ip,tcp);
						jxta = fragments.get(tcpFlowId);

						if(jxta != null){
							// writes actual payload into last payload
							ByteArrayOutputStream buffer = new ByteArrayOutputStream();
							buffer.write(jxta.getJxtaPayload(), 0, jxta.getJxtaPayload().length);					
							buffer.write(tcp.getPayload(), 0, tcp.getPayload().length);
							ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.toByteArray());
							try{
								jxta.decode(packet,byteBuffer);								
								updateHalfSocketFlows(jxta,pipeReqst,dataFlows,ackFlows);
								fragments.remove(tcpFlowId);
								if(byteBuffer.hasRemaining()){
									try{
										jxta = new Jxta();
										packet.getHeader(jxta);
										byte[] rest = new byte[byteBuffer.remaining()];
										byteBuffer.get(rest, 0, byteBuffer.remaining());
										byteBuffer = ByteBuffer.wrap(rest);
										jxta.decode(packet,byteBuffer);										
										updateHalfSocketFlows(jxta,pipeReqst,dataFlows,ackFlows);
										if(byteBuffer.hasRemaining()){
											try{
												jxta = new Jxta();
												packet.getHeader(jxta);
												byte[] rest2 = new byte[byteBuffer.remaining()];
												byteBuffer.get(rest2, 0, byteBuffer.remaining());
												jxta.decode(packet,ByteBuffer.wrap(rest2));										
												updateHalfSocketFlows(jxta,pipeReqst,dataFlows,ackFlows);												
											}catch(BufferUnderflowException e ){
												ArrayList<JPacket> packets = jxta.getPackets();
												packets.clear();
												packets.add(packet);
												fragments.put(tcpFlowId,jxta);										
											}catch (IOException failed) {
												ArrayList<JPacket> packets = jxta.getPackets();
												packets.clear();
												packets.add(packet);
												fragments.put(tcpFlowId,jxta);
											}
											return;
										}
									}catch(BufferUnderflowException e ){
										ArrayList<JPacket> packets = jxta.getPackets();
										packets.clear();
										packets.add(packet);
										fragments.put(tcpFlowId,jxta);										
									}catch (IOException failed) {
										ArrayList<JPacket> packets = jxta.getPackets();
										packets.clear();
										packets.add(packet);
										fragments.put(tcpFlowId,jxta);
									}catch (Exception e) {
										e.printStackTrace();
									}
									return;
								}															
							}catch(BufferUnderflowException e ){								
								jxta.getPackets().add(packet);
								fragments.put(tcpFlowId, jxta);
							}catch (IOException failed) {
								jxta.getPackets().add(packet);
								fragments.put(tcpFlowId, jxta);
							}catch (Exception e) {
								e.printStackTrace();
							}
							return;
						}
					}

					if (packet.hasHeader(Jxta.ID)) {
						jxta = new Jxta();
						packet.getHeader(jxta);
						if(jxta.getJxtaMessageType() == JxtaMessageType.DEFAULT){
							try{									
								jxta.decodeMessage();								
								updateHalfSocketFlows(jxta,pipeReqst,dataFlows,ackFlows);
								if(jxta.isFragmented()){									
									ByteBuffer remain = ByteBuffer.wrap(jxta.getRemain());									
									jxta = new Jxta();
									packet.getHeader(jxta);
									jxta.decode(remain);
									jxta.getPackets().add(packet);
									updateHalfSocketFlows(jxta,pipeReqst,dataFlows,ackFlows);
									if(jxta.isFragmented()){									
										remain = ByteBuffer.wrap(jxta.getRemain());									
										jxta = new Jxta();
										packet.getHeader(jxta);
										jxta.decode(remain);
										updateHalfSocketFlows(jxta,pipeReqst,dataFlows,ackFlows);
									}
								}
							}catch(BufferUnderflowException e ){
								Ip4 ip = new Ip4();
								packet.getHeader(ip);
								int id = JxtaUtils.getFlowId(ip,tcp);								
								jxta.setFragmented(true);
								jxta.getPackets().add(packet);
								fragments.put(id,jxta);
							}catch(IOException e){
								Ip4 ip = new Ip4();
								packet.getHeader(ip);
								int id = JxtaUtils.getFlowId(ip,tcp);	
								jxta.setFragmented(true);
								jxta.getPackets().add(packet);
								fragments.put(id,jxta);
							}catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			}

		}, errbuf);
	}

	@SuppressWarnings("rawtypes")
	public static void updateHalfSocketFlows(Jxta jxta, SortedMap<Integer,Jxta> pendings, SortedMap<Integer,JxtaSocketFlow> dataFlows, SortedMap<Integer,JxtaSocketFlow> ackFlows){
		MessageElement el = null;
		ElementIterator elements;
		Message msg = jxta.getMessage();

		try{
			// jxta-reliable-block
			elements = msg.getMessageElements(Defs.NAMESPACE, Defs.MIME_TYPE_BLOCK);
			if(elements.hasNext()){
				EndpointRouterMessage erm = new EndpointRouterMessage(msg,false);
				String strPipeId = erm.getDestAddress().getServiceParameter();
				Integer pipeId = Integer.valueOf(strPipeId.hashCode());

				if(dataFlows.containsKey(pipeId)){
					dataFlows.get(pipeId).addJxtaData(jxta);
				}else{
					throw new RuntimeException("Jxta data flow shoulf exists");
				}					
			}else{
				el = msg.getMessageElement(Defs.ACK_ELEMENT_NAME);
				if(el != null){
					EndpointRouterMessage erm = new EndpointRouterMessage(msg,false);
					String strPipeId = erm.getDestAddress().getServiceParameter();
					Integer pipeId = Integer.valueOf(strPipeId.hashCode());
					
					if(ackFlows.containsKey(pipeId)){
						ackFlows.get(pipeId).addJxtaAck(jxta);
					}else{
						throw new RuntimeException("Jxta data flow shoulf exists");
					}
				}else{
					el = msg.getMessageElement(socketNamespace, reqPipeTag);
					if(el != null){
						XMLDocument adv = (XMLDocument) StructuredDocumentFactory.newStructuredDocument(el);
						PipeAdvertisement pipeAdv = (PipeAdvertisement) AdvertisementFactory.newAdvertisement(adv);					
						String strPipeId = new String(pipeAdv.getID().toString());
						int pipeId = strPipeId.hashCode();
						pendings.put(pipeId, jxta);
					}else{
						el = msg.getMessageElement(socketNamespace, remPipeTag);
						if (el != null) {
							XMLDocument adv = (XMLDocument) StructuredDocumentFactory.newStructuredDocument(el);
							PipeAdvertisement pipeAdv = (PipeAdvertisement) AdvertisementFactory.newAdvertisement(adv);

							EndpointRouterMessage erm = new EndpointRouterMessage(msg,false);

							String strDataPipeId = new String(pipeAdv.getID().toString());//createdPipe
							String strAckPipeId = erm.getDestAddress().getServiceParameter();//usedPipe

							int dataPipeId = strDataPipeId.hashCode();
							int ackPipeId = strAckPipeId.hashCode();

							Jxta reqPipe = pendings.remove(ackPipeId);

							JxtaSocketFlow socketFlow = new JxtaSocketFlow(dataPipeId, ackPipeId);
							socketFlow.addJxtaData(reqPipe);
							socketFlow.addJxtaAck(jxta);
							dataFlows.put(dataPipeId, socketFlow);
							ackFlows.put(ackPipeId, socketFlow);
						}else{
							el = msg.getMessageElement(socketNamespace, closeTag);
							if(el != null){
								EndpointRouterMessage erm = new EndpointRouterMessage(msg,false);
								String strPipeId = erm.getDestAddress().getServiceParameter();
								Integer pipeId = Integer.valueOf(strPipeId.hashCode());

								if(dataFlows.containsKey(pipeId)){
									dataFlows.get(pipeId).addJxtaData(jxta);
								}else
									if(ackFlows.containsKey(pipeId)){
										ackFlows.get(pipeId).addJxtaAck(jxta);
									}else{
										throw new RuntimeException("Unexpected pipeId");
									}
							}
						}	
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}	

	private static void printFlowStatistics(Context ctx, SortedMap<Integer,JxtaSocketFlow> dataFlows, SortedMap<Integer,JxtaSocketFlow> ackFlows){
		//		final Text jxtaPayloadKey = new Text("JXTA_Payload");
		//		final Text jxtaTimeKey = new Text("JXTA_Time");
		//		final Text jxtaThroughputKey = new Text("JXTA_Throughput");
		final Text jxtaRelyRttKey = new Text("JXTA_Reliability_RTT");		

		//		final SortedMapWritable payOutput = new SortedMapWritable();
		//		final SortedMapWritable timeOutput = new SortedMapWritable();
		//		final SortedMapWritable throughput = new SortedMapWritable();
		final SortedMapWritable rttOutput = new SortedMapWritable();
		int uncompleted = 0;

		for (Integer dataFlowKey : dataFlows.keySet()) {			
			final JxtaSocketFlow dataFlow = dataFlows.get(dataFlowKey);	
			//			payOutput.put(new LongWritable(stats.getEndTime()), new DoubleWritable((stats.getPayload())/1024));			
			//			timeOutput.put(new DoubleWritable(dataFlowKey), new DoubleWritable(stats.getTransferTime()));
			//			throughput.put(new DoubleWritable(dataFlowKey), new DoubleWritable((stats.getPayload()/1024)/(stats.getTransferTime()/1000)));
			if(dataFlow.isAckComplete() && dataFlow.isDataComplete()){				
				SortedMap<Integer,Long> rtts = dataFlow.getRtts();
				for (Integer num : rtts.keySet()) {
					LongWritable key = new LongWritable(dataFlow.getEndTime() + num);														
					rttOutput.put(key, new DoubleWritable(rtts.get(num)));
				}
			}else{
				uncompleted++;
			}
		}
		System.out.println("### Total Uncompleted Flows = " + uncompleted);
		try{
			//			ctx.write(jxtaPayloadKey, payOutput);
			//			ctx.write(jxtaTimeKey, timeOutput);
			//			ctx.write(jxtaThroughputKey, throughput);
			ctx.write(jxtaRelyRttKey, rttOutput);			
		}catch(IOException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}