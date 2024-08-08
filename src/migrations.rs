use anyhow::Result;
use neo4rs::{query, Graph};
use std::fs;
use std::io::prelude::*;
use std::path::Path;
use tracing::info;

pub async fn apply_migrations(graph: &Graph) -> Result<()> {
    // Ensure the migration node exists
    ensure_migration_node(graph).await?;

    let mut entries: Vec<_> = fs::read_dir("./migrations")?
        .filter_map(Result::ok)
        .collect();

    entries.sort_by_key(|entry| entry.path());

    let current_migration_number = get_current_migration_number(graph).await?;

    for entry in entries {
        if let Some(extension) = entry.path().extension() {
            if extension == "cypher" {
                let migration_number = extract_migration_number(&entry.path())?;
                if migration_number > current_migration_number {
                    run_migration(graph, &entry.path()).await?;
                    update_migration_number(graph, migration_number).await?;
                }
            }
        }
    }

    Ok(())
}

async fn ensure_migration_node(graph: &Graph) -> Result<()> {
    let statement = r#"
        MERGE (m:Migration {id: 1})
        ON CREATE SET m.latest_migration = 0
    "#;
    graph.run(query(statement)).await?;
    Ok(())
}

async fn get_current_migration_number(graph: &Graph) -> Result<i32> {
    let statement = r#"
        MATCH (m:Migration {id: 1})
        RETURN m.latest_migration AS latest_migration
    "#;
    let mut result = graph.execute(query(statement)).await?;
    if let Some(row) = result.next().await? {
        let latest_migration: i32 = row.get("latest_migration")?;
        Ok(latest_migration)
    } else {
        Ok(0)
    }
}

async fn update_migration_number(graph: &Graph, migration_number: i32) -> Result<()> {
    let statement = r#"
        MATCH (m:Migration {id: 1})
        SET m.latest_migration = $migration_number
    "#;
    graph
        .run(query(statement).param("migration_number", migration_number))
        .await?;
    Ok(())
}

fn extract_migration_number(path: &Path) -> Result<i32> {
    let file_stem = path.file_stem().unwrap().to_str().unwrap();
    let parts: Vec<&str> = file_stem.split('_').collect();
    let migration_number = parts[0].parse::<i32>()?;
    Ok(migration_number)
}

async fn run_migration(graph: &Graph, path: &Path) -> Result<()> {
    let mut file = fs::File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let statements = contents.split(";");

    for statement in statements {
        if statement.trim().is_empty() {
            continue;
        }

        graph.run(query(statement)).await?;
        info!("Migration applied: {:?}", path.display());
    }

    Ok(())
}
