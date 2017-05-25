# Basic QueryableService example

1) Build and deploy the 'BasicApp' Service Fabric application.  The stateless service 'Basic.WebSvc' will listening on port 80 for queries.

2) Perform a GET request for http://localhost/query/BasicApp/ProductSvc/$metadata to see the OData metadata about the 'ProductSvc' stateful service.  This will show the reliable collections and their value types.

3) Perform a GET request for http://localhost/query/BasicApp/ProductSvc/products?$top=5 to see 5 Product values from the service.

4) Perform GET requests against http://localhost/query/BasicApp/ProductSvc/products?<OData-query> using the OData query language ($top, $filter, $orderby, $select).
