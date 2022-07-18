import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import pipeline_builder.Covid19HashtagPipeline;

public class hello_hazelcast {

    public static void main(String[] args) {
        JetInstance jet = null;
        try {
            jet = Jet.bootstrappedInstance();
            Covid19HashtagPipeline covid19HashtagPipeline = new Covid19HashtagPipeline();
            jet.newJob(covid19HashtagPipeline.buildPipeline()).join();
        }
        finally {
            if(jet!=null) jet.shutdown();
        }
    }

}