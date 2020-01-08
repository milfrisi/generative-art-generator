from app.directories import project_dir

db_init_scripts = [
    project_dir / "src/app/component/sql/create_db.sql",
    project_dir / "src/app/component/sql/table_ticker.sql",
]
