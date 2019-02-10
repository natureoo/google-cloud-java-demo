package demo;

import java.util.Map;

/**
 * @author chenj
 * @date 2019-02-03 17:11
 * @email 924943578@qq.com
 */
public class TestEnv {
    
    public static void main(String[] args){
        Map<String, String> envs = System.getenv();
        for(String key : envs.keySet()){
            System.out.println(key + " : "+ envs.get(key));
        }
    }
}
