package TASK2;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.Map;
import PAM.Point;
public class CandidateSetofMedoids {
	private Map<Integer,ArrayList<Integer>> candidateset;
	private int noofclusters;
	private String filepath="file1.txt";
	private ArrayList<Point> data;
	public CandidateSetofMedoids() throws Exception
	{
		File f=new File(filepath);
		FileReader fr=new FileReader(f);
		BufferedReader br=new BufferedReader(fr);
		String line=br.readLine();
		data=new ArrayList<>();
		while(line!=null)
		{
			String[] splitline=line.split(",");
			Point p=new Point(Double.parseDouble(splitline[0]),Double.parseDouble(splitline[1]));
			data.add(p);
			line=br.readLine();
		}
		br.close();
		fr.close();
		Random r = new Random();
		int i=0;
		 candidateset = new HashMap<>();
		while(i<100)
		{
			i++;
			ArrayList<Integer> MedoidPosition=(ArrayList<Integer>) r.ints(noofclusters, 0, data.size()).boxed().collect(Collectors.toList());
			candidateset.put(i, MedoidPosition);
		}
	}
	public Map<Integer,ArrayList<Integer>> getcandidateset(){
		return this.candidateset;
	}
	public ArrayList<Point> getdata(){
		return this.data;
	}
}
