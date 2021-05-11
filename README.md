# Large Scale Data Processing: Final Project
## Contributors: Jacob Kennedy, Nick Bearup, Jack Wiseman
## Results
|           File name           |        Number of edges       |           Run Time            |        Number of Matchings       |
| ----------------------------- | ---------------------------- | ----------------------------- | -------------------------------- | 
| com-orkut.ungraph.csv         | 117185083                    | 5528 seconds - ~92 min.       | 1,390,939 matchings              |
| twitter_original_edges.csv    | 63555749                     | 2408 seconds - ~40 min.       | 94,074    matchings              |
| soc-LiveJournal1.csv          | 42851237                     | 3253 seconds - ~52 min.       | 1,712,331 matchings              |
| soc-pokec-relationships.csv   | 22301964                     | 1491 seconds - ~24 min.       | 649091    matchings              |
| musae_ENGB_edges.csv          | 35324                        | 24 seconds                    | 2427      matchings
| log_normal_100.csv            | 2671                         | 19 seconds                    | 49        matchings              |

## Final Report:
We had a few main goals going into the project. Our top priority was to produce high quality matchings. With this mind, we determined that it would be appropriate to use the color coding algorithm. The following is the pseudocode:

Color-Coding Algorithm:

repeat 22k+1(k+1)logk iterations{
	Each vertex color itself in red or blue with prob ½

	Let G = (V,E), where
		V = {u | u ∈ V is free, or ∃(u,v) ∈ M s.t. (u,v) is bichromatic}
		E = {(u,v) | (u,v) ∈ E, u, v ∈ V and (u,v) is bichromatic}

	Let C be a set of maximal augmenting path of length ≤ 2k-1
	Augment along paths in C

This algorithm runs 22k+1(k+1)logk iterations, and each iteration uses O(k3logn) rounds. While this would not give us the perfect maximum matching, it would give us solid results. The concern was, however, given that we only had limited time for this project, we wanted some confidence that we could implement the algorithm. This particular algorithm would be rather difficult to accomplish, so we considered some other algorithms. 

We then considered implementing either the bidding variant of luby’s algorithm or the Israeli-Itai algorithm to get a valid maximal matching and then deal with the augmenting paths afterwards. We considered the fact that they both use O(logn) rounds and that it is unclear as to which would perform better, so we had to come up with some other criteria to determine which to implement. Ultimately, since we have implemented a variant of the bidding variant of Luby algorithm in the past, we figured it would be better to further our understanding of a different algorithm, so we decided to implement a variant of Israeli Itai algorithm. 

The following pseudocode is where we began:

Israeli-Itai Algorithm

M = ∅
while(there exist active vertices){
	Every active vertex propose to a random neighbor

	Every active vertex accept an arbitrary proposal, if there is any

	Every active vertex randomly generate 0 or 1. A proposed edge from u to v join M if u generated 0 and v generated 1

	De-active the endpoints of those who joined M
}

## Additional Advantages of Israeli-Itai Algorithm:
Israeli-Itai algorithm was good for the task at hand because of the following: A good vertex, v, gets deleted with constant probability running through the algorithm. A good vertex is one that has at least d(v)/3 neighbors of equal or lower degrees, so each time a vertex is proposed, it will be deleted with a constant probability, ¼ the probability of its proposition. Based on Luby’s Algorithm, we know that at least half the edges are incident to ‘good' vertices, and the Israeli-Itai algorithm removes a constant fraction of edges each round in expectation. Because of this, in log(n) rounds, it is very likely that the graph will become empty.
 
## Problems + Our Solution:
It was difficult with aggregate messages to make each vertex send a message to only one neighbor at random. To solve this, we had each vertex send a message to every neighbor. The message contains both the vertex Id and the int 1. We then implemented the following merge algorithm:
(a,x), (b,y) => select (a,x+y) with probability (a/x + y), otherwise select (b,x+y).

We did this by selecting a random float between 0 and 1 and choosing (a,x+y) if the float was smaller than (a/x+y). Doing this allowed each vertex to receive one message from a neighbor at random. We then had only to send a message back to the vertex, effectively having picked the original vertex at random. 
We had to make sure that each vertex only received the message from one neighbor during the next aggregate messages. This wasn’t difficult, but we needed to add the neighbor to M only if it was ultimately chosen by the receiving vertex. To accomplish this task, we had each vertex send its vertex ID. That way, when the neighbor selects one message to receive, it will have the corresponding vertex as its attribute. The remaining vertices would set their attribute to -1. At that point, we simply had to filter the graph so that it has only vertices with attributes != -1 and turn that vertex rdd into an edge rdd. After that, we added those edges and any new vertices to M. 

## Plan for Augmenting Paths:
We had a couple of ideas for the implementation of the final step of the project. However, upon getting valid maximal matchings, we also had some concerns about space with our code. We calculated the amount of remaining time that we had for our project, and determined that it would be prudent to focus our time on the space issue so that we would not run into problems with the larger files. 

## Estimate of Amount of Computation for each Case:

log_normal_100.csv - the program ran for 19 seconds on a 2x4 N1 standard core CPU in the the Google Cloud Platform (n1-standard-4)

musae_ENGB_edges.csv - the program ran for 24 seconds on a 2x4 N1 high-memory core CPU in the GCP (n1-highmem-4)

soc-pokec-relationships.csv - the program ran for 1491 seconds on a 2x16 N1 high-memory core CPU in the GCP (n1-highmem-16)

soc-LiveJournal1.csv - the program ran for 3253 seconds on a 2x16 N1 high-memory core CPU in the GCP (n1-highmem-16)

twitter_original_edges.csv - the program ran for 2408 seconds on a 2x16 N1 high-memory core CPU in the GCP (n1-highmem-16)

com-orkut.ungraph.csv - the program ran for 5528 seconds on a 2x32 N1 high-memory core CPU in the GCP (n1-highmem-32)
