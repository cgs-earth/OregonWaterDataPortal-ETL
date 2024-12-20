# Access FROST directly without going through nginx so we can avoid issues with
# the reverse proxy blocking or timing out requests
API_BACKEND_URL = "http://localhost:8080/FROST-Server/v1.1"
