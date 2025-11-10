use super::{PersistenceResult, ScheduleStore};
use crate::{Schedule, ScheduleMetadata, Task};
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::Mutex;

pub struct SqliteScheduleStore {
    connection: Mutex<Connection>,
}

impl SqliteScheduleStore {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> PersistenceResult<Self> {
        let connection = Connection::open(path)?;
        Self::initialize_schema(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn initialize_schema(connection: &Connection) -> PersistenceResult<()> {
        let ddl = r#"
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS schedule_metadata (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                metadata_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                task_json TEXT NOT NULL
            );
        "#;
        connection.execute_batch(ddl)?;
        Ok(())
    }

    fn save_metadata(
        &self,
        tx: &rusqlite::Transaction,
        metadata: &ScheduleMetadata,
    ) -> PersistenceResult<()> {
        let json = serde_json::to_string(metadata)?;
        tx.execute("DELETE FROM schedule_metadata", [])?;
        tx.execute(
            "INSERT INTO schedule_metadata (id, metadata_json) VALUES (1, ?1)",
            params![json],
        )?;
        Ok(())
    }

    fn save_tasks(&self, tx: &rusqlite::Transaction, schedule: &Schedule) -> PersistenceResult<()> {
        tx.execute("DELETE FROM tasks", [])?;
        let df = schedule.dataframe();
        let mut stmt = tx.prepare("INSERT INTO tasks (id, task_json) VALUES (?1, ?2)")?;
        for row_idx in 0..df.height() {
            let task = Task::from_dataframe_row(df, row_idx)?;
            let json = serde_json::to_string(&task)?;
            stmt.execute(params![task.id, json])?;
        }
        Ok(())
    }
}

impl ScheduleStore for SqliteScheduleStore {
    fn save_schedule(&self, schedule: &Schedule) -> PersistenceResult<()> {
        super::validate_schedule(schedule)?;
        let mut conn = self.connection.lock().expect("sqlite mutex poisoned");
        let tx = conn.transaction()?;
        self.save_metadata(&tx, schedule.metadata())?;
        self.save_tasks(&tx, schedule)?;
        tx.commit()?;
        Ok(())
    }

    fn load_schedule(&self) -> PersistenceResult<Option<Schedule>> {
        let conn = self.connection.lock().expect("sqlite mutex poisoned");

        let mut stmt = conn.prepare("SELECT metadata_json FROM schedule_metadata WHERE id = 1")?;
        let metadata_json_opt: Option<String> = stmt.query_row([], |row| row.get(0)).optional()?;

        let Some(metadata_json) = metadata_json_opt else {
            return Ok(None);
        };

        let metadata: ScheduleMetadata = serde_json::from_str(&metadata_json)?;

        let mut stmt = conn.prepare("SELECT task_json FROM tasks ORDER BY id ASC")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut tasks = Vec::new();
        for json in rows {
            let json = json?;
            let task: Task = serde_json::from_str(&json)?;
            tasks.push(task);
        }

        super::validate_tasks(&tasks)?;

        let mut schedule = Schedule::new_with_metadata(metadata);
        for task in tasks {
            schedule.upsert_task_record(task)?;
        }

        Ok(Some(schedule))
    }
}
