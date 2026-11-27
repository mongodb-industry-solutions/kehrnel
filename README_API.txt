==== definition/template APIs ===

curl.exe -X POST `
  'http://localhost:8080/api/domains/openehr/definition/template/adl1.4' `
  -H 'Content-Type: application/xml' `
  -H 'x-active-env: staging' `
  -H 'x-api-key: THIS_IS_A_TEST' `
  --data-binary '@src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/templates/sample_laboratory_v0_4.opt'


curl.exe -X GET `
  'http://localhost:8080/api/domains/openehr/definition/template/adl1.4' `
  -H 'Content-Type: application/xml' `
  -H 'x-active-env: staging' `
  -H 'x-api-key: THIS_IS_A_TEST'


curl.exe -X GET `
  'http://localhost:8080/api/domains/openehr/definition/template/adl1.4/sample_laboratory_v0.4' `
  -H 'Content-Type: application/xml' `
  -H 'x-active-env: staging' `
  -H 'x-api-key: THIS_IS_A_TEST'

======================================

==== synthetic/jobs APIs ===

curl.exe -X POST `
  'http://localhost:8080/environments/staging/synthetic/jobs' `
  -H 'accept: application/json' `
  -H 'Content-Type: application/json' `
  -d '{
    \"domain\": \"openehr\",
    \"payload\": {
      \"patient_count\": 10,
      \"templates\": [
        {\"template_id\": \"sample_laboratory_v0.4\", \"min_per_patient\": 1, \"max_per_patient\": 3}
      ]
    }
  }'


curl.exe -X GET `
  'http://localhost:8080/environments/staging/synthetic/jobs' `
  -H 'accept: application/json' `
  -H 'Content-Type: application/json'

======================================
