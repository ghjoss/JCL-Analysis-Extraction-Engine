-- PostgreSQL Version of JCL Analysis Schema (Updated with relative_step)

-- Project Metadata Table
CREATE TABLE PROJECTS (
    project_id SERIAL PRIMARY KEY,
    project_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Steps Table
CREATE TABLE STEPS (
    project_id INTEGER NOT NULL REFERENCES PROJECTS(project_id),
    step_id INTEGER NOT NULL,
    relative_step VARCHAR(8) NOT NULL, -- Format: Xnnnnnnn
    step_name VARCHAR(8),
    proc_step_name VARCHAR(8),
    program_name VARCHAR(8),
    proc_name VARCHAR(8),
    parameters TEXT,
    cond_logic TEXT,
    PRIMARY KEY (project_id, step_id)
);

-- Data Allocations Table
CREATE TABLE DATA_ALLOCATIONS (
    project_id INTEGER NOT NULL,
    step_id INTEGER NOT NULL,
    ds_id INTEGER NOT NULL,
    dd_name VARCHAR(8),
    allocation_offset INTEGER NOT NULL DEFAULT 1,
    dsn VARCHAR(44),
    disp_status VARCHAR(8),
    disp_normal VARCHAR(8),
    disp_abnormal VARCHAR(8),
    unit VARCHAR(8),
    vol_ser VARCHAR(6),
    is_dummy BOOLEAN DEFAULT FALSE,
    instream_ref TEXT,
    lrecl VARCHAR(10),
    blksize VARCHAR(10),
    recfm VARCHAR(8),
    dcb_attributes JSONB,
    PRIMARY KEY (project_id, step_id, ds_id),
    FOREIGN KEY (project_id, step_id) REFERENCES STEPS(project_id, step_id)
);

-- Performance Indexes
CREATE INDEX idx_steps_project ON STEPS(project_id);
CREATE INDEX idx_dd_step ON DATA_ALLOCATIONS(project_id, step_id);