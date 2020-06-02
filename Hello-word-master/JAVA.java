package com.alticeusa.audiencepartners.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smartystreets.api.exceptions.BatchFullException;
import com.smartystreets.api.us_street.Batch;
import com.smartystreets.api.us_street.Candidate;
import com.smartystreets.api.us_street.Client;
import com.smartystreets.api.us_street.ClientBuilder;
import com.smartystreets.api.us_street.Components;
import com.smartystreets.api.us_street.Lookup;
import com.smartystreets.api.us_street.Metadata;

public class StandardizeBatchAddresses extends GenericUDF {

	private static final Logger logger = LoggerFactory.getLogger(StandardizeBatchAddresses.class);
	private int counter = 0;
	private Long processed = Long.valueOf(0);
	private static final String SMARTYSTREETS_URL_PROPERTY="smartystreets.url";
	private static final String AUTH_ID_PROPERTY="auth.id";
	private static final String AUTH_TOKEN_PROPERTY="auth.token";
	
	Client client = null;
	private Batch batch = new Batch();

	public StandardizeBatchAddresses() {
//		client = new ClientBuilder("f9b1a4d6-5c4b-7665-1581-09a4511373a5", "n2FSgxq3OtiPqQzXoiwH").withUrl("https://us-street.api.smartystreets.com/street-address").build();
		Properties props = new Properties();
		try {
			logger.info("Loading config properties file");
			props.load(getClass().getResourceAsStream("/config.properties"));
			logger.info("Properties loaded "+props);
			client = new ClientBuilder(props.getProperty(AUTH_ID_PROPERTY), props.getProperty(AUTH_TOKEN_PROPERTY)).withUrl(props.getProperty(SMARTYSTREETS_URL_PROPERTY)).build();
		} catch (IOException e) {
			logger.error("Error reading the config properties file", e);
			throw new RuntimeException(e);
		}
	}

	PrimitiveObjectInspector streeti;
	PrimitiveObjectInspector secondaryi;
	PrimitiveObjectInspector cityi;
	PrimitiveObjectInspector zipi;
	PrimitiveObjectInspector statei;
	PrimitiveObjectInspector batchsizei;
	PrimitiveObjectInspector partitionCounti;
	
	private ArrayList ret = new ArrayList();

	@Override
	public String getDisplayString(String[] argument) {
		return "Standardize Address";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

		if (args[0] == null) {
			throw new UDFArgumentTypeException(0, "No Street is mentioned");
		}
		if (args[1] == null) {
			throw new UDFArgumentTypeException(0, "No secondary is mentioned");
		}
		if (args[2] == null) {
			throw new UDFArgumentTypeException(0, "No zip is mentioned");
		}
		if (args[3] == null) {
			throw new UDFArgumentTypeException(0, "No city is mentioned");
		}
		if (args[4] == null) {
			throw new UDFArgumentTypeException(0, "No state is mentioned");
		}
		if (args[5] == null) {
			throw new UDFArgumentTypeException(0, "No batch size is mentioned");
		}
		if (args[6] == null) {
			throw new UDFArgumentTypeException(0, "No partition count is mentioned");
		}

	
		streeti = (PrimitiveObjectInspector) args[0];
		secondaryi = (PrimitiveObjectInspector) args[1];
		cityi = (PrimitiveObjectInspector) args[3];
		zipi = (PrimitiveObjectInspector) args[2];
		statei = (PrimitiveObjectInspector) args[4];
		batchsizei = (PrimitiveObjectInspector) args[5];
		partitionCounti = (PrimitiveObjectInspector) args[6];
		ret = new ArrayList();

		ArrayList structFieldNames = new ArrayList();
		ArrayList structFieldObjectInspectors = new ArrayList();

		structFieldNames.add("std_delivery_line_1");
		structFieldNames.add("std_delivery_line_2");
		structFieldNames.add("std_zipcode");
		structFieldNames.add("std_city");
		structFieldNames.add("std_state");
		structFieldNames.add("input_street");
		structFieldNames.add("input_secondary");
		structFieldNames.add("input_zip");
		structFieldNames.add("input_city");
		structFieldNames.add("input_state");

		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

		StructObjectInspector si2 = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
				structFieldObjectInspectors);

		ListObjectInspector li2;
		li2 = ObjectInspectorFactory.getStandardListObjectInspector(si2);
		return li2;
	}

	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {

		logger.info("standardizing address fields "+Arrays.asList(args));
		
		Object oin1 = args[0].get();
		String street1 = (String) streeti.getPrimitiveJavaObject(oin1);
		Object oin6 = args[1].get();
		String secondary = (String) secondaryi.getPrimitiveJavaObject(oin6);
		Object oin2 = args[2].get();
		String zip1 = (String) zipi.getPrimitiveJavaObject(oin2);
		Object oin3 = args[3].get();
		String city1 = (String) cityi.getPrimitiveJavaObject(oin3);
		Object oin4 = args[4].get();
		String state1 = (String) statei.getPrimitiveJavaObject(oin4);
		Object oin5 = args[5].get();
		int batchsize = (Integer) batchsizei.getPrimitiveJavaObject(oin5);
		Object oin7 = args[6].get();
		long partitionCount = 0;
		if (partitionCounti.getPrimitiveJavaObject(oin7) instanceof Integer)
		{
			partitionCount = (Integer) partitionCounti.getPrimitiveJavaObject(oin7);
		}
		else
		{
			partitionCount = (Long) partitionCounti.getPrimitiveJavaObject(oin7);
		}
		

		logger.info("address passed, street=" + street1 + ",zip=" + zip1 + ",city=" + city1 + ",state=" + state1);
		counter++;
		processed++;
		
		
		try {
			Lookup lookup = new Lookup();
			lookup.setStreet(street1);
			lookup.setSecondary(secondary);
			lookup.setCity(city1);
			lookup.setState(state1);
			lookup.setZipCode(zip1);
			lookup.setMaxCandidates(1);
			batch.add(lookup);
			logger.info("batch size is "+batch.size());
		} catch (BatchFullException ex) {
			logger.error(ex.getMessage(), ex);
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

        if (counter == batchsize || (partitionCount - processed) == Long.valueOf(0)) {
			ret.clear();
			logger.info("batch is full calling the api");
			try {
				logger.info("Batch size is "+batch.size()+" Processed "+processed+" Remaining "+(partitionCount - processed));
				logger.info("batch input street " + batch.get(0).getStreet());
				try {
					client.send(batch);
 
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					throw e;
				}

				Vector<Lookup> lookups = batch.getAllLookups();

				for (int i = 0; i < batch.size(); i++) {
					ArrayList<Candidate> candidates = lookups.get(i).getResult();
					Object[] e;
					e = new Object[10];
					
					if (candidates.isEmpty()) {
						logger.warn("No standard address found for input address fields -> street "+batch.get(i).getStreet()+", secondary "+batch.get(i).getSecondary()+", zip "+batch.get(i).getZipCode()+", city "+batch.get(i).getCity()+", state "+batch.get(i).getState());
						logger.warn("Address " + i + " is invalid.\n");
						e[0] = null;
						e[1] = null;
						e[2] = null;
						e[3] = null;
						e[4] = null;						
						e[5] = batch.get(i).getStreet() == null ? null: new Text(batch.get(i).getStreet());
						e[6] = batch.get(i).getSecondary() == null ? null: new Text(batch.get(i).getSecondary());
						e[8] = batch.get(i).getCity() == null ? null: new Text(batch.get(i).getCity());
						e[9] = batch.get(i).getState() == null ? null: new Text(batch.get(i).getState());
						e[7] = batch.get(i).getZipCode() == null ? null: new Text(batch.get(i).getZipCode());
						
						ret.add(e);
						continue;
					}

					logger.info("Address " + i + " is valid. (There is at least one candidate)");

					for (Candidate candidate : candidates) {
						final Components components = candidate.getComponents();
						final Metadata metadata = candidate.getMetadata();

						logger.info("\nCandidate " + candidate.getCandidateIndex() + ":");
						logger.info("Delivery line 1: " + candidate.getDeliveryLine1());
						logger.info("Delivery line 2:       " + candidate.getDeliveryLine2());
						logger.info("ZIP Code:        " + components.getZipCode());
						logger.info("City:          " + components.getCityName());
						logger.info("State:        " + components.getState());
					}

					e[0] = new Text(candidates.get(0).getDeliveryLine1());
					if (candidates.get(0).getDeliveryLine2() != null)
						e[1] = new Text(candidates.get(0).getDeliveryLine2());
					else
						e[1] = null;
					e[3] = candidates.get(0).getComponents().getCityName() == null ? null : new Text(candidates.get(0).getComponents().getCityName());
					e[2] = candidates.get(0).getComponents().getZipCode() == null ? null : new Text(candidates.get(0).getComponents().getZipCode());
					e[4] = candidates.get(0).getComponents().getState() == null ? null : new Text(candidates.get(0).getComponents().getState());
					e[5] = batch.get(i).getStreet() == null ? null: new Text(batch.get(i).getStreet());
					e[6] = batch.get(i).getSecondary() == null ? null: new Text(batch.get(i).getSecondary());
					e[8] = batch.get(i).getCity() == null ? null: new Text(batch.get(i).getCity());
					e[9] = batch.get(i).getState() == null ? null: new Text(batch.get(i).getState());
					e[7] = batch.get(i).getZipCode() == null ? null: new Text(batch.get(i).getZipCode());
					ret.add(e);
				}
				counter = 0;
				batch.clear();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			logger.info("returning batch containing "+ret.size()+ " records");
			return ret;

		} else {
			return null;
		}

	}
	
	public static void main(String[] args) throws IOException, HiveException {
		
		StandardizeBatchAddresses standardizeAddresses = new StandardizeBatchAddresses();
		
	
		PrimitiveObjectInspector objIns1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		PrimitiveObjectInspector objIns2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		PrimitiveObjectInspector objIns3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		PrimitiveObjectInspector objIns4 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;		
		PrimitiveObjectInspector objIns5 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		PrimitiveObjectInspector objIns6 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		PrimitiveObjectInspector objIns7 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		ObjectInspector[] arg_names = new ObjectInspector[] {objIns1, objIns2, objIns3, objIns4, objIns6, objIns6, objIns7};
		standardizeAddresses.initialize(arg_names);
		
//		DeferredObject obj1 = new DeferredJavaObject("700 W MARKET ST");
//		DeferredObject obj2 = new DeferredJavaObject("356112457");
//		DeferredObject obj3 = new DeferredJavaObject("ATHENS");
//		DeferredObject obj4 = new DeferredJavaObject("AL");
//		DeferredObject obj5 = new DeferredJavaObject(1);
//		DeferredObject obj6 = new DeferredJavaObject(null);
		
		DeferredObject obj1 = new DeferredJavaObject(args[0]);
		DeferredObject obj2 = new DeferredJavaObject(args[1]);
		DeferredObject obj3 = new DeferredJavaObject(args[2]);
		DeferredObject obj4 = new DeferredJavaObject(args[3]);
		DeferredObject obj5 = new DeferredJavaObject(args[4]);
		DeferredObject obj6 = new DeferredJavaObject(1);
		DeferredObject obj7 = new DeferredJavaObject(1);
		
		DeferredObject[] udfArgs = new DeferredObject[] {obj1, obj2, obj3, obj4, obj5, obj6, obj7};
		Object[] result = (Object[])((List)standardizeAddresses.evaluate(udfArgs)).get(0);
		
		System.out.println("result -> street="+result[0]+", zip="+result[2]);
		
	}


}
