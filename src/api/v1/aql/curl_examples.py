##############################
### Create a Stored Query ####
##############################

# This is an upsert operation, it means that if the query already exists, it will update the query sentence in case it's different
# It won't say that the query already exists

curl -XPUT http://localhost:9000/v1/query/com.kehrnel_test::ehr_summary/v1 \
-H "Content-Type: text/plain" \
--data-raw 'SELECT e/ehr_id/value, e/time_created/value FROM EHR e'

Expected Response: An empty body with a 201 Created status code,

#################################
###### GET a Stored Query #######
#################################

curl -X GET http://localhost:9000/v1/query/com.kehrnel::ehr_summary/v1

Expected Response: The raw AQL string in the body with a 200 OK status code.

#############################
## List all stored queries ##
#############################

curl -X GET http://localhost:9000/v1/query

# Expected Response: A JSON array with a 200 OK status code.

##########################
## Execute an AQL Query ##
##########################

curl -X POST http://localhost:9000/v1/query/aql \
-H "Content-Type: application/json" \
-d '{
  "q": "SELECT e/ehr_id/value, e/system_id/value FROM EHR e"
}'

# Expected Response: A full QueryResponse JSON object with the results.

####################################
### Execute AQL with Pagination ####
####################################

curl -X POST http://localhost:9000/v1/query/aql \
-H "Content-Type: application/json" \
-d '{
  "q": "SELECT e/ehr_id/value FROM EHR e",
  "offset": 1,
  "fetch": 2
}'

# Expected Response: A QueryResponse object containing a maximum of 2 rows, skipping the first one.


########################################
### Execute AQL Filtering by ehr_id ####
########################################

curl -X POST http://localhost:9000/v1/query/aql \
-H "Content-Type: application/json" \
-d '{
  "q": "SELECT e/ehr_id/value, e/time_created/value FROM EHR e",
  "ehr_id": "63f85ba7-b65e-479d-8542-e5607cb3eadd"
}'

# Expected Response: A QueryResponse object containing exactly one row corresponding to the specified ehr_id.


##############################
### Delete a stored query ####
##############################

curl -X DELETE -v http://Localhost:9000/v1/query/com.kehrnel::ehr_summary/v1

# Expected Response: You should see < HTTP/1.1 204 No Content in the output, with no response body.