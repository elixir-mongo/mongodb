defmodule AWSX509.Example do
  def connect do
    cert_dir = "/home/username/certs/"
    {:ok, conn} = Mongo.start_link(
      database: "database",
      hostname: "mongodb.company.com",
      username: "CN=username,OU=unit,O=company,L=Location,ST=State,C=US",
      password: "foo",  # needs a dummy string. but would be nice if it could ignore this for X509
      ssl: true,
      auth_mechanism: :x509,
      ssl_opts: [
        ciphers: ['AES256-GCM-SHA384'],  # needed to connect to AWS
        cacertfile: Path.join([cert_dir, "rootca.pem"]),
        certfile: Path.join([cert_dir, "mycert.pem"])
      ]
    )
    conn
  end
end