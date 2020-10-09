        package com.saurav.kafka.tutorial2;

        import com.google.common.collect.Lists;
        import com.twitter.hbc.ClientBuilder;
        import com.twitter.hbc.core.Client;
        import com.twitter.hbc.core.Constants;
        import com.twitter.hbc.core.Hosts;
        import com.twitter.hbc.core.HttpHosts;
        import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
        import com.twitter.hbc.core.processor.StringDelimitedProcessor;
        import com.twitter.hbc.httpclient.auth.Authentication;
        import com.twitter.hbc.httpclient.auth.OAuth1;
        import org.apache.kafka.common.protocol.types.Field;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.util.List;
        import java.util.concurrent.BlockingQueue;
        import java.util.concurrent.LinkedBlockingQueue;
        import java.util.concurrent.TimeUnit;

        public class TwitterProducer {
            Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());
            String consumerKey="QDj3PgyOyyWS1B9awqkIlB8SZ";
            String consumerSecret="fJvu2pPAwR2O0So0OfNHik1Nzp7ZKX1OahANoyRzNo0XvnevKt";
            String token="901364277478543361-let6znrrF5rg38tTVzvtcCmbQX0OmXN";
            String secret="Zw9fC1yxxfnyfTQbnaLQ8HZ4QUuCzaSZ6hznau8vKCGUL";


            public TwitterProducer(){

            }
            public static void main(String[] args) {
                //System.out.println(" Twitter Producer ");
                new TwitterProducer().run();

            }
            public void run (){
                logger.info("Setup");
                BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

                // create a twitter client
                // Attempts to establish a connection.

                Client client=createTwitterClient(msgQueue);

                client.connect();
                logger.info("Data is gettng proceesesd");


                // create a kafka producer
                // loop to send tweets to kafka producer

                while (!client.isDone()) {
                    String msg=null;
                    try {
                         msg = msgQueue.poll(5, TimeUnit.SECONDS);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                        client.stop();
                    }
                    if (msg!=null){

                    logger.info(msg);
                    }

                }
                logger.info("End of the Application ");


            }

            public Client createTwitterClient(BlockingQueue<String>msgQueue){

                /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
                Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
                StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

                List<String> terms = Lists.newArrayList("CSK","MI");
                hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
                Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret,token,secret);

                ClientBuilder builder = new ClientBuilder()
                        .name("Hosebird-Client-01")                              // optional: mainly for the logs
                        .hosts(hosebirdHosts)
                         .authentication(hosebirdAuth)
                        .endpoint(hosebirdEndpoint)
                        .processor(new StringDelimitedProcessor(msgQueue));



                Client hosebirdClient = builder.build();
                return hosebirdClient;


            }
        }
