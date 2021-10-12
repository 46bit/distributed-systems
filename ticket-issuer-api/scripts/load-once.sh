ghz --insecure --async --proto api/api.proto --call api.TicketIssuer/IssueTicket \
  --connections=4 -n 5100 --rps=300 -d '{"user_id": "{{.RequestNumber}}"}' ":$1"