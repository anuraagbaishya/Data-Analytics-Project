package PAM;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import PAM.Point;
public class Kmedoids{
	private int maxiterations;
	private int noofclusters;
	private double finaldistance=0;
	public Kmedoids(int a,int b) {
		this.maxiterations=a;
		this.noofclusters=b;
	}
	public void PAM(ArrayList<Point> data) {
		Random r=new Random();
		
		
		ArrayList<Integer> MedoidPosition=(ArrayList<Integer>) r.ints(noofclusters, 0, data.size()).boxed().collect(Collectors.toList());
		Double[][] DistanceMatrix = calculateDistanceMatrix(MedoidPosition, data);
		Map<Integer,ArrayList<Integer>> cluster=calculatecluster(DistanceMatrix, MedoidPosition, data.size());
		finaldistance = calculateDistance(DistanceMatrix, cluster);
		ArrayList<Integer> DonePoints=new ArrayList<>();
		for(int a=0;a<data.size();a++)
			DonePoints.add(a);
		for(int i:MedoidPosition)
		{
			for(int a=0;a< DonePoints.size();a++)
			{
				if(i==DonePoints.get(a))
				{
					DonePoints.remove(a);
					break;
				}
			}
		}
		int iteration=0;
		double newdistance=0.0;
		while(DonePoints.size()!=0||iteration!=maxiterations)
		{
			iteration++;
			int newposition=getRandomPoint(DonePoints);
			int newpoint=DonePoints.get(newposition);
			DonePoints.remove(newposition);
			for(int i=0;i<MedoidPosition.size();i++ )
			{
				ArrayList<Integer> newlist= MedoidPosition; 
				newlist.add(i,newpoint);
				Double[][] newDistanceMatrix=calculateDistanceMatrix(newlist, data);
				Map<Integer,ArrayList<Integer>> newcluster=calculatecluster(newDistanceMatrix, newlist, data.size());
				newdistance=calculateDistance(newDistanceMatrix, newcluster);
				if (newdistance<finaldistance)
				{
					finaldistance=newdistance;
					cluster = newcluster;
					MedoidPosition=newlist;
					DistanceMatrix=newDistanceMatrix;
					break;
					
				}
			}
		}
		
		
	}
	public static Double[][] calculateDistanceMatrix(ArrayList<Integer> MedoidPosition,ArrayList<Point> data)
	{
		Double[][] DistanceMatrix =new Double[data.size()][];
		for(Integer a: MedoidPosition) {
			for(int i=0;i<data.size();i++)
			{
				DistanceMatrix[a][i] = data.get(a).distance(data.get(a),data.get(i));
			}
		}
		return DistanceMatrix;
	}
	public static Map<Integer,ArrayList<Integer>> calculatecluster(Double[][] DistanceMatrix,ArrayList<Integer> MedoidPosition,int length){
		Map<Integer,ArrayList<Integer>> cluster=new HashMap<>();
		for(Integer a:MedoidPosition)
			cluster.put(a,new ArrayList<Integer>());
		for(int i=0;i<length;i++)
		{	Double Mindistance = DistanceMatrix[MedoidPosition.get(0)][i];
			int medoid=MedoidPosition.get(0);
			for(Integer a: MedoidPosition) {
				if (DistanceMatrix[a][i]<Mindistance) {
					Mindistance=DistanceMatrix[a][i];
					medoid = a;
				}
					
			}
			ArrayList<Integer> list=cluster.get(medoid);
			list.add(i);
			cluster.put(medoid, list);
		}
		return cluster;
	}
	public static Double calculateDistance(Double[][] DistanceMatrix,Map<Integer,ArrayList<Integer>> cluster) {
		Iterator iterator = cluster.keySet().iterator();
		Double distance=0.0;
	    while (iterator.hasNext()) {
	            int key = Integer.parseInt(iterator.next().toString());
	            ArrayList<Integer> value = cluster.get(key);
	            for(int a:value)
	                distance=distance+DistanceMatrix[key][a];       
	}
	    return distance;
	}
	public static int getRandomPoint(ArrayList<Integer> donePoints) {
		Random r=new Random();
		int a= r.nextInt(donePoints.size());
		return a;
	}
	
}