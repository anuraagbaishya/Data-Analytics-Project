package PAM;

public class Point {
	private Double x;
	private Double y;
	public Point(Double a,Double b) {
		this.x=a;
		this.y=b;
	}
	public Double distance(Point p,Point q)
	{
		return Math.sqrt(Math.pow(p.getx()-q.getx(),2)+Math.pow(p.gety()-q.gety(),2));
	}
	public Double getx() {
		return this.x;
	}
	public Double gety() {
		return this.y;
	}
}
