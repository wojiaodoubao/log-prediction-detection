package motif;
/**
 * 序列数据离散化后的单位元素
 * Raw data经过预处理之后得到的序列数据应该是一个Meta[]数组
 * */
public abstract class Meta {
	@Override
	public abstract boolean equals(Object m);
	public abstract String toString();
}