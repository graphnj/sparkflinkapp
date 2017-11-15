import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhujinhua on 2017/11/15.
 */
public class TopicCRUD {

    String brokerlist = null;
    static TopicCRUD init(String brokerlist){
        TopicCRUD topicCRUD = new TopicCRUD();
        topicCRUD.brokerlist = brokerlist;
        return topicCRUD;
    }
    //创建topic
    void createTopic(String topic ) {
        ZkUtils zkUtils = ZkUtils.apply(brokerlist, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 创建一个单分区单副本名为t1的topic
        AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }


    //删除topic
    void deleteTopic(String topic) {
        ZkUtils zkUtils = ZkUtils.apply(brokerlist, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 删除topic 't1'
        AdminUtils.deleteTopic(zkUtils, topic);
        zkUtils.close();
    }
    //   比较遗憾地是，不管是创建topic还是删除topic，目前Kafka实现的方式都是后台异步操作的，而且没有提供任何回调机制或返回任何结果给用户，所以用户除了捕获异常以及查询topic状态之外似乎并没有特别好的办法可以检测操作是否成功。

    //查询topic
    void queryTopic(String topic) {
        ZkUtils zkUtils = ZkUtils.apply(brokerlist, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 获取topic 'test'的topic属性属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        // 查询topic-level属性
        Iterator it = props.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key + " = " + value);
        }
        zkUtils.close();
    }

    //修改topic
    void updateTopic(String topic) {
        ZkUtils zkUtils = ZkUtils.apply(brokerlist, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test");
        // 增加topic级别属性
        props.put("min.cleanable.dirty.ratio", "0.3");
        // 删除topic级别属性
        props.remove("max.message.bytes");
        // 修改topic 'test'的属性
        AdminUtils.changeTopicConfig(zkUtils, topic, props);
        zkUtils.close();
    }

}
