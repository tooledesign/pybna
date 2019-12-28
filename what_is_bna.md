# What is the BNA?

Network connectivity can be a difficult concept to describe, understand, and --
crucially -- to measure. Traditionally, cities attempting to quantify the
usefulness of their bike networks have fallen back on easily-measured attributes
like the mileage of bike lanes, or an as-the-crow flies distance to the nearest
bike facility.

Though there may be some correlation between "bike friendliness" and these crude
measures, they fail to capture the importance of having an _interconnected_
network of comfortable bike routes, in addition to the role that _destinations_
play in connecting people to places. The BNA aims to capture the importance of
the interconnectedness of bicycle routes by measuring access to destinations.

There are three steps to the BNA (not counting the process of assembling the
necessary input data as described in the [import instructions](import.md)).

1. Level of Traffic Stress
2. Block-to-block connectivity analysis
3. Aggregation of destinations

## Level of Traffic Stress

Level of Traffic Stress (LTS) is a concept first described by Peter Furth, Maaza
Mekuria, and Hilary Nixon in a paper published by the Mineta Transportation
Institute at San Jose State University. In the paper, they set forth roadway
conditions that contribute to cyclist stress. These are things you'd probably
expect: high traffic speeds are uncomfortable for bicycling, quiet residential
streets are less stressful than busy arterial roadways,etc. The original paper
is published
[here](https://transweb.sjsu.edu/research/low-stress-bicycling-and-network-connectivity).

One important aspect discussed in the paper is the critical role intersection
crossings play in route continuity. It's easy to forget the importance of
intersections when looking at a map of bicycle routes because the intersections
themselves take virtually no space. Contrary to the visuals, intersections
actually have an outsize importance in terms of routes a typical cyclist might
feel comfortable following. If you've ever biked down a bucolic residential
street and found you had to turn back at the virtual wall of high-speed traffic
zooming down the arterial at the end of the block, you understand this concept
well.

Another significant topic discussed in the paper is the principle of the
"weakest link". This is an assumption that, in the minds of most cyclists, the
highest stress level encountered on a route defines the stress level of the
entire route. In the words of the authors, _"If people will not use links whose
stress exceeds their tolerance, **several low-stress links cannot compensate for
one high-stress link**."_ The implication of this is significant for network
planning as the prevailing attitude has been to provide comfortable routes where
feasible but accept some compromises in constrained locations (such as busy
intersections). This is usually done in the hope that users will stomach a
short, uncomfortable segment as long as the rest of the route is pleasant.

The BNA adopts the general concepts of LTS for rating the comfort of possible
route options. The lookup tables used as a basis for the BNA are published on
Peter Furth's academic website
[here](http://www.northeastern.edu/peter.furth/criteria-for-level-of-traffic-stress/).
There are some deviations from Dr Furth's tables, including for features not
covered in his work, such as HAWK signals. We've also made some adjustments at
the margins to better fit conditions as we've observed them doing bike network
planning around North America.

The main magic of using LTS in the BNA is that it elevates the importance of
intersections and insists on a continuous, comfortable route. Fortunately, the
BNA is flexible enough to allow you to make your own calls on some of these
issues. Has your city already completed an LTS analysis? You can plug it
directly into the BNA, essentially skipping this step entirely. Would you prefer
to use a more nuanced scheme with five different stress levels? The BNA can
accommodate that too.

## Block-to-block connectivity analysis

The core of the BNA measures low-stress connections between "blocks". Under the
default settings, a block is a US Census block, which is roughly analogous to a
city block. There's no magic to the composition of a block, but coarser areal
units such as census block groups are less well-suited for use in the BNA since
their large area can paper over connectivity problems occuring within their
boundaries.

The process for measuring connectivity can be summarized as follows:

1. Associate roads with blocks (i.e. decide which roads belong to which blocks)
2. Iterate over each block and identify the shortest route to all other blocks regardless of the stress level
3. Iterate over each block and identify the shortest route to all other blocks on only low-stress portions of the network

The results of #2 and #3 are then compared. If a connection is found in #3 but
it requires a significant detour compared to the baseline condition in #2 that
connection is ignored.

The end result is a matrix of block-to-block connections that identifies which
blocks are connected to which other blocks via a low-stress route.

## Aggregation of destinations

With the block-to-block connectivity established, the BNA then looks at
destinations. It starts by measuring the universe of possible destinations
around each block regardless of stress, and comparing that to the subset of
destinations accessible via a low-stress route. This process can be summarized:

1. Associate destinations with blocks
2. Iterate over each block and count the number of destinations accessible regardless of stress level
3. Iterate over each block and count the number of destinations accessible via a low-stress route

This yields two numbers -- a count of low-stress destinations and a count of all
destinations -- that can be compared for each category of destination. As the
number of low-stress destinations approaches the total number of destinations, a
block's score approaches 100/100. As the number approaches zero, the block's
score approaches zero too. The score is ultimately dependent on the total number
of destinations around each blocks. Blocks with very few destinations nearby are
judged based on how well they connect to those destinations on low-stress
routes. For example, a block which is connected to two schools will score higher
if there are only two possible schools to connect to (full points) than if there
are five possible schools to connect to.

This can be moderated by defining a set thresholds at which points are awarded.
In the school example above, one could decide that access to two schools is
sufficient to warrant a 100/100 score regardless of how many additional schools
are nearby. This is done through an explicit configuration of the school
destinations in the [configuration file](config.md).

Destination categories are also completely flexible. You may decide that the
destinations you want to measure access to include coffee shops and movie
theaters. As long as data can be supplied for a destination type, it can be
incorporated into BNA results.

The last step is combining the scores for all destination categories into a
single BNA score. This is accomplished by weights assigned to each destination
category. A higher weight means that category's score has a larger impact on the
overall score. One caveat to this can result in surprising scores: if no
destinations of a given category are identified within reasonable distance of a
given block, that category's score is not factored into the overall score. For
example, if a block does not have any universities nearby, the university score
for that block will be empty and the block's overall score will omit that
category from consideration in the overall score. This often results in blocks
in outlying areas receiving very high scores because they are surrounded by few
destinations, all of which happen to be accessible via a low-stress route. There
are ways to soften the effects of this with some post-processing of the results
(e.g. manually reducing scores for blocks who don't meet a certain threshold for
total number of destinations). As of yet these operations haven't been codified
into the pyBNA code base.
