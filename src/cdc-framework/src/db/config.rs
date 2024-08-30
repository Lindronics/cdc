#[derive(Debug)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

impl DbConfig {
    pub fn connection_string(&self, replication: bool) -> String {
        let mut s = format!(
            "user={} password={} host={} port={} dbname={}",
            self.user, self.password, self.host, self.port, self.dbname
        );
        if replication {
            s.push_str(" replication=database");
        }
        s
    }
}

#[derive(Debug)]
pub struct ReplicationConfig {
    pub table: String,
    pub publication: String,
    pub replication_slot: String,
}
