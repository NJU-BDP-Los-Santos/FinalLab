package rankPRvalue;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparator;

public  class DescSort extends WritableComparator{

    public DescSort() {
        super(FloatWritable.class,true);//注册排序组件
    }
    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
                       int arg4, int arg5) {
        return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);//注意使用负号来完成降序
    }

    @Override
    public int compare(Object a, Object b) {

        return   -super.compare(a, b);//注意使用负号来完成降序
    }
}