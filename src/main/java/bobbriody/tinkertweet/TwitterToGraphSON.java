package bobbriody.tinkertweet;


import oauth.signpost.OAuthConsumer;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

public class TwitterToGraphSON
{
    enum Type
    {
        object,
        array
    }

    // Twitter properties to save
    static final String[] props = {"name", "location", "screen_name", "description"};

    static final String outputPath = "tinkertweet.json";

    private final String[] seeds;

    private final Graph g = TinkerGraph.open();
    private final OAuthConsumer consumer;

    public TwitterToGraphSON(final String consumerKey, final String consumerSecret, final String accessToken, final String accessSecret, final String[] seeds)
    {
        this.seeds = seeds;

        // Initialize the twitter consumer
        consumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret);
        consumer.setTokenWithSecret(accessToken, accessSecret);
    }

    /**
     * The main run method that orchestrates the entire process
     * @throws Exception
     */
    public void run() throws Exception
    {
        // Grab the list of followed users for each seed user.
        for (String seed : seeds)
        {
            ingestFollowing(seed);
        }

        // Populate the profiles with info like name and location
        populateProfiles();

        // save graph to GraphSON
        File output = new File(outputPath);
        try (final OutputStream os = new FileOutputStream(output))
        {
            GraphSONWriter.build().create().writeGraph(os, g);
            System.out.println("GraphSON file successfully saved to: " + output.getAbsolutePath());
        }
    }

    /**
     * Helper method that executes a request against the Twitter API.
     * @param url
     * @param t
     * @return
     * @throws Exception
     */
    private Object request(String url, Type t) throws Exception
    {
        HttpGet request = new HttpGet("https://api.twitter.com/1.1/" + url);
        consumer.sign(request);

        HttpClient client = new DefaultHttpClient();
        HttpResponse response = client.execute(request);

        int statusCode = response.getStatusLine().getStatusCode();
        System.out.println(url + "\n\t" + statusCode + ":" + response.getStatusLine().getReasonPhrase());
        if (t == Type.object)
        {
            return new JSONObject(IOUtils.toString(response.getEntity().getContent()));
        }
        else
        {
            return new JSONArray(IOUtils.toString(response.getEntity().getContent()));
        }
    }


    /**
     * Iterate the users and populate the profiles in batches
     * @throws Exception
     */
    private void populateProfiles() throws Exception
    {
        int ii = 0;
        StringBuilder ids = new StringBuilder();
        GraphTraversal<Vertex, Object> t = g.traversal().V().values("user_id");
        while (t.hasNext())
        {
            ii++;
            long id = (long) t.next();
            ids.append(id).append(",");
            if (ii == 99 || t.hasNext() == false)
            {
                populateProfiles(ids);
                ids = new StringBuilder();
                ii = 0;
            }
        }
    }

    /**
     * Populate the profiles for a batch of users
     * @param ids
     * @throws Exception
     */
    private void populateProfiles(StringBuilder ids) throws Exception
    {
        JSONArray profiles = (JSONArray) request("users/lookup.json?include_entities=false&user_id=" + ids, Type.array);
        for (int ii = 0; ii < profiles.length(); ii++)
        {
            JSONObject p = profiles.getJSONObject(ii);
            // Fearlessly expecting all vertices to exist, hence no hasNext() on this traversal.
            Vertex v = g.traversal().V().has("user_id", p.getLong("id")).next();

            for (String prop : props)
            {
                if (!p.isNull(prop))
                {
                    v.property(prop, p.get(prop));
                }
            }
        }

    }

    /**
     * Accepts a seed screen name and grabs the list of twitter IDs that the seed follows.
     * @param seedScreenName
     * @throws Exception
     */
    private void ingestFollowing(String seedScreenName) throws Exception
    {
        // Grab info for the specified screen_name
        JSONObject seedJson = (JSONObject) request("users/show.json?screen_name=" + seedScreenName, Type.object);

        // create seed vertex
        Vertex seed = getOrCreate(seedJson.getLong("id"));

        // get following ids
        JSONObject followingResp = (JSONObject) request("friends/ids.json?screen_name=" + seedScreenName, Type.object);
        JSONArray ids = followingResp.getJSONArray("ids");
        for (int ii = 0; ii < ids.length(); ii++)
        {
            seed.addEdge("follows", getOrCreate(ids.getLong(ii)));
        }
    }

    /**
     * Aaaaa... good old get or create :)
     * @param id
     * @return
     */
    private Vertex getOrCreate(Long id)
    {
        GraphTraversal<Vertex, Vertex> vt = g.traversal().V().has("user_id", id);
        if (vt.hasNext())
        {
            return vt.next();
        }
        Vertex v = g.addVertex("person");
        v.property("user_id", id);
        return v;
    }

    public static void main(String[] args) throws Exception
    {
        File config = new File("config.properties");
        if (!config.exists() || config.isDirectory())
        {
            System.err.println("Error: Expected to find ./config.properties file. Perhaps you should copy config.properties.template and fill in your Twitter API info.");
            System.exit(1);
        }

        try
        {
            Properties props = new Properties();
            props.load(new FileInputStream(config));

            new TwitterToGraphSON(
                    props.getProperty("consumerKey"),
                    props.getProperty("consumerSecret"),
                    props.getProperty("accessToken"),
                    props.getProperty("accessSecret"),
                    props.getProperty("screen_names").split(",")
            ).run();

        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
