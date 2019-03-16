# Flexible Persistence Microservice

Are you moving to microservices, but you've found that maintaining data
consistency across separate services is like, complicated and stuff? The easiest
solution is to use a big old shared database, but some nerd on hackernews is
giving you mumbo jumbo about "coupling". Besides, you're not rewriting your
whole app in microservices just to end up relying on legacy technologies like
PostgreSQL and MongoDB.

Clearly what you need is a database microservice, aka a DaaS (Database as a
Service). Our proprietary Service Query Language empowers you to build and send
type safe, structured queries over GRPC:

	statement, err := 
		Select("person", "full_name").
		OrderBy(Col("birth"), grpcdbpb.OrderingDirection_DESC).
		Statement()
	result, err := client.Query(ctx, statement)
