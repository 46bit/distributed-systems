mkdir -p tmp
go build -o ./tmp/ticket-issuer-api .
ls scripts/config*.yml | parallel -j 20 --lb -X ./tmp/ticket-issuer-api