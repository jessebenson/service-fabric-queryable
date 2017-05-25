# Basic QueryableService example

BasicApp is a Service Fabric application with a stateless front-end service (Basic.WebSvc) and a partitioned stateful service (Basic.ProductSvc).

The products stateful service has an IReliableDictionary<string, Product> collection, and it derives from QueryableService (which is a StatefulService) to enable query support.

The web stateless service exposes a Web API for querying any QueryableService in the cluster, by adding an ApiController that derives from QueryableController and implementing two Web APIs.

# Running the sample

1) Build and deploy the 'BasicApp' Service Fabric application.  The stateless service 'Basic.WebSvc' will listening on port 80 for queries.

2) Perform a GET request for http://localhost/query/BasicApp/ProductSvc/$metadata to see the OData metadata about the 'ProductSvc' stateful service.  This will show the reliable collections and their value types.

3) Perform a GET request for http://localhost/query/BasicApp/ProductSvc/products?$top=5 to see 5 Product values from the service.

4) Perform GET requests against http://localhost/query/BasicApp/ProductSvc/products?<OData-query> using the OData query language ($top, $filter, $orderby, $select).  For example:  "http://localhost/query/BasicApp/ProductSvc/products?$top=10&$filter=Quantity ge 2 and Quantity le 4&$select=Sku,Price"
