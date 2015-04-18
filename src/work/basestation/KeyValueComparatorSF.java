package work.basestation;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Map.Entry<String,Float> 比较
 * 参考 http://tbwuming.iteye.com/blog/1873634 的代码，谢谢这位朋友分享
 * @author yrx
 *
 */
public class KeyValueComparatorSF implements Comparator<Map.Entry<String,Float>> {
        public enum Type {
                KEY, VALUE;
        }

        public enum Order {
                ASC, DESC
        }

        private Type type;
        private Order order;

        public KeyValueComparatorSF(Type type, Order order) {
                this.type = type;
                this.order = order;
        }

        @Override
        public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
                switch (type) {
                case KEY:
                        switch (order) {
                        case ASC:
                                return o1.getKey().compareTo(o2.getKey());
                        case DESC:
                                return o2.getKey().compareTo(o1.getKey());
                        default:
                                throw new RuntimeException("顺序参数错误");
                        }
                case VALUE:
                        switch (order) {
                        case ASC:
                                return o1.getValue().compareTo(o2.getValue());
                        case DESC:
                                return o2.getValue().compareTo(o1.getValue());
                        default:
                                throw new RuntimeException("顺序参数错误");
                        }
                default:
                        throw new RuntimeException("类型参数错误");
                }
        }
        public int compare(Float f1,Float f2){
                return f1.compareTo(f2);
        }
}
