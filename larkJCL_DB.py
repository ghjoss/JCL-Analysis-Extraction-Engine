import re
import os
import json
import copy
import psycopg2
from psycopg2.extras import Json
from lark import Lark, Transformer, v_args

# =============================================================================
# JCL PREPROCESSOR
# =============================================================================

class JCLPreprocessor:
    def __init__(self, config):
        self.config = config
        self.system_type = config.get("SYSTEM", "LWM")
        self.symbol_table = {}
        self.procedure_map = {}
        path_val = config.get("PATH")
        lib_val = config.get("LIB", [])
        self.lib_paths = ([path_val] if path_val else []) + (lib_val if isinstance(lib_val, list) else [])
        
        self.re_job_admin = re.compile(r"//([A-Z$#@][A-Z0-9$#@]{0,7})?\s+(JOB|CNTL|ENDCNTL|EXPORT|NOTIFY|OUTPUT|SCHEDULE|JCLLIB|SET)(\s+|$)", re.IGNORECASE)
        self.re_cond = re.compile(r"//([A-Z$#@][A-Z0-9$#@]{0,7})?\s*(IF|THEN|ELSE|ENDIF)(\s+|$)", re.IGNORECASE)
        self.re_proc_start = re.compile(r"//([A-Z$#@][A-Z0-9$#@]{0,7})\s+PROC(\s+|$)", re.IGNORECASE)
        self.re_pend = re.compile(r"//\s+PEND(\s+|$)", re.IGNORECASE)
        self.re_include = re.compile(r"//\s+INCLUDE\s+MEMBER=([A-Z$#@][A-Z0-9$#@]{0,7})", re.IGNORECASE)
        self.re_exec = re.compile(r"//([A-Z$#@][A-Z0-9$#@]{0,7})?\s+EXEC\s+(PGM=|PROC=)?([A-Z$#@][A-Z0-9$#@]{0,7})", re.IGNORECASE)
        self.re_dd_instream = re.compile(r"//([A-Z$#@][A-Z0-9$#@]{0,7})?\s+DD\s+(\*|DATA)", re.IGNORECASE)

    def resolve_path(self, member_name):
        for base in self.lib_paths:
            if not base: continue
            if self.system_type == "Z":
                path = f"{base}({member_name})"
            else:
                ext = self.config.get("EXT", "")
                filename = f"{member_name}.{ext}" if ext else member_name
                path = os.path.join(base, filename)
            if self.system_type != "Z" and os.path.exists(path):
                return path
            elif self.system_type == "Z":
                return path
        return None

    def clean_line(self, line):
        line = line.rstrip('\n').rstrip('\r')
        line = line[:72]
        if line.startswith("//*") or line.startswith("/*") or line.strip() == "//":
            return None
        return line

    def strip_jcl_comment(self, line, is_continuation):
        if is_continuation:
            content = line.lstrip('/')
            prefix = ""
            operands_and_comment = content.lstrip()
        else:
            parts = line.split(None, 2)
            if len(parts) < 3: return line.rstrip()
            prefix = " ".join(parts[:2])
            operands_and_comment = parts[2]
        in_quotes = False
        end_idx = len(operands_and_comment)
        for i, char in enumerate(operands_and_comment):
            if char == "'": in_quotes = not in_quotes
            elif char == " " and not in_quotes:
                end_idx = i
                break
        operands = operands_and_comment[:end_idx]
        return (prefix + " " + operands).strip() if prefix else operands.strip()

    def parse_params(self, param_string):
        params = {}
        if not param_string: return params
        parts, current, in_quotes = [], [], False
        for char in param_string:
            if char == "'": in_quotes = not in_quotes
            if char == "," and not in_quotes:
                parts.append("".join(current)); current = []
            else:
                current.append(char)
        parts.append("".join(current))
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                params[k.strip().upper()] = v.strip().strip("'")
        return params

    def expand_procedure(self, proc_name, exec_stmt, outer_label):
        proc_data = self.procedure_map.get(proc_name.upper())
        if not proc_data:
            path = self.resolve_path(proc_name)
            if path:
                try:
                    with open(path, 'r') as f:
                        lines = f.readlines()
                        proc_data = {"header": lines[0], "body": lines[1:]}
                except: pass
        if not proc_data: return [exec_stmt]
        
        local_symbols = copy.deepcopy(self.symbol_table)
        header_stmt = proc_data["header"]
        if " PROC " in header_stmt.upper():
            proc_part = header_stmt.upper().split("PROC", 1)[1]
            local_symbols.update(self.parse_params(proc_part))
        
        exec_operands = exec_stmt.split("EXEC", 1)[1].strip()
        match = re.search(r"[,\s]", exec_operands)
        if match:
            param_str = exec_operands[match.start()+1:].strip()
            local_symbols.update(self.parse_params(param_str))
            
        old_symbols = self.symbol_table
        self.symbol_table = local_symbols
        expanded = self.process_line_list(proc_data["body"])
        self.symbol_table = old_symbols
        
        # Wrap expanded steps with metadata for the parser context
        return [f"*PROC_START* label={outer_label} proc={proc_name}"] + expanded + ["*PROC_END*"]

    def process_line_list(self, lines):
        statements = []
        current_statement = ""
        is_continuing = False
        idx = 0
        while idx < len(lines):
            line_raw = lines[idx]; line = self.clean_line(line_raw); idx += 1
            if line is None: continue
            cleaned_content = self.strip_jcl_comment(line, is_continuing)
            ends_with_comma = cleaned_content.endswith(",")
            current_statement += cleaned_content
            if ends_with_comma:
                is_continuing = True; continue
            is_continuing = False
            stmt = self.apply_symbolics(current_statement); current_statement = ""
            
            proc_match = self.re_proc_start.search(stmt)
            if proc_match:
                proc_name, proc_header, proc_body = proc_match.group(1).upper(), stmt, []
                while idx < len(lines):
                    p_line_raw = lines[idx]
                    p_line_cleaned = self.clean_line(p_line_raw)
                    if p_line_cleaned and self.re_pend.search(p_line_cleaned):
                        idx += 1; break
                    proc_body.append(p_line_raw); idx += 1
                self.procedure_map[proc_name] = {"header": proc_header, "body": proc_body}
                continue
                
            if "JCLLIB " in stmt.upper(): self.update_lib_paths(stmt); continue
            if " SET " in stmt.upper() or stmt.startswith("// SET "): self.update_symbols(stmt); continue
            if self.re_job_admin.search(stmt) or self.re_cond.search(stmt): continue
            
            include_match = self.re_include.search(stmt)
            if include_match:
                path = self.resolve_path(include_match.group(1).upper())
                if path: statements.extend(self.preprocess_file(path))
                continue
                
            exec_match = self.re_exec.search(stmt)
            if exec_match:
                is_explicit_pgm = "PGM=" in (exec_match.group(2) or "").upper()
                name = exec_match.group(3).upper()
                outer_label = exec_match.group(1) or ""
                if not is_explicit_pgm and (name in self.procedure_map or self.resolve_path(name)):
                    # suppressing the original EXEC card as it's not a step, just a call
                    statements.extend(self.expand_procedure(name, stmt, outer_label))
                    continue
            
            if self.re_dd_instream.search(stmt):
                statements.append(stmt)
                dlm = "/*"; dlm_match = re.search(r"DLM=([^\s,]{2})", stmt, re.IGNORECASE)
                if dlm_match: dlm = dlm_match.group(1).replace("'", "").replace('"', "")
                while idx < len(lines):
                    p_line_raw = lines[idx].rstrip()
                    if (dlm == "/*" and (p_line_raw.startswith("//") or p_line_raw.startswith("/*"))) or \
                       (dlm != "/*" and p_line_raw.startswith(dlm)):
                        if dlm != "/*" and p_line_raw.startswith(dlm): idx += 1
                        break
                    statements.append(f"*PAYLOAD* {p_line_raw[:72]}"); idx += 1
                continue
            statements.append(stmt)
        return statements

    def preprocess_file(self, file_path):
        if not file_path: return []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            return self.process_line_list(lines)
        except Exception: return []

    def apply_symbolics(self, stmt):
        for sym in sorted(self.symbol_table.keys(), key=len, reverse=True):
            val = str(self.symbol_table[sym])
            stmt = stmt.replace(f"&{sym}..", f"{val}.").replace(f"&{sym}.", val).replace(f"&{sym}", val)
        return stmt

    def update_symbols(self, stmt):
        match = re.search(r"SET\s+([A-Z0-9$#@]{1,8})=([^,\s]+)", stmt, re.IGNORECASE)
        if match: self.symbol_table[match.group(1).upper()] = match.group(2).strip("'")

    def update_lib_paths(self, stmt):
        match = re.search(r"ORDER=\((.*?)\)", stmt, re.IGNORECASE)
        if match:
            new_paths = [p.strip().strip("'").strip('"') for p in match.group(1).split(",")]
            self.lib_paths = new_paths + self.lib_paths

# =============================================================================
# LARK GRAMMAR & TRANSFORMER
# =============================================================================

JCL_GRAMMAR = r"""
    ?start: line+
    ?line: exec_statement | unnamed_exec | dd_statement | unnamed_dd

    exec_statement: "//" JCL_ID OP_EXEC exec_content
    unnamed_exec: "//" OP_EXEC exec_content
    dd_statement: "//" JCL_ID OP_DD dd_params
    unnamed_dd: "//" OP_DD dd_params

    ?exec_content: (pos_proc ["," exec_params]) | exec_params
    pos_proc: VALUE

    exec_params: exec_param ("," exec_param)* [","]
    dd_params: dd_param ("," dd_param)* [","]

    ?exec_param: pgm_param | proc_param_kw | parm_param | symbolic_override | exec_keyword
    ?dd_param: dsn_param | disp_param | space_param | dcb_param | vol_param 
             | lrecl_param | recfm_param | blksize_param | dsorg_param
             | instream_star | instream_data | dummy_param | keyword_param

    pgm_param: PGM_KW EQ VALUE
    proc_param_kw: PROC_KW EQ VALUE
    parm_param: PARM_KW EQ (VALUE | JCL_QUOTED_STRING)
    
    ?dsn_param: DSN_KW EQ dsn_value
    ?dsn_value: (temp_dsn | referback | symbolic_val | VALUE) [gdg_suffix]
    temp_dsn: "&&" JCL_ID
    referback: "*." JCL_ID ("." JCL_ID)?
    symbolic_val: "&" JCL_ID ["."]
    gdg_suffix: "(" ["+" | "-"] NUMBER ")"

    symbolic_override: JCL_ID EQ (VALUE | JCL_QUOTED_STRING)
    exec_keyword: EXEC_KW EQ (VALUE | list_val | JCL_QUOTED_STRING)

    !disp_param: DISP_KW EQ ( DISP_VAL | "(" [DISP_VAL] ("," [DISP_VAL])* ")" )
    !space_param: SPACE_KW EQ "(" SPACE_UNIT "," space_quantities ["," "RLSE"] ["," "CONTIG"] ["," "ROUND"] ")"
    space_quantities: NUMBER | "(" NUMBER ("," NUMBER)* ")"
    
    vol_param: VOL_KW EQ vol_sublist
    !vol_sublist: SER_KW EQ (VALUE | list_val)

    dcb_param: DCB_KW EQ ( VALUE | "(" dcb_sublist ")" )
    dcb_sublist: dcb_subitem ("," dcb_subitem)*
    ?dcb_subitem: lrecl_param | recfm_param | blksize_param | dsorg_param | symbolic_override

    lrecl_param: LRECL_KW EQ NUMBER
    recfm_param: RECFM_KW EQ RECFM_VALUE
    blksize_param: BLKSIZE_KW EQ NUMBER
    dsorg_param: DSORG_KW EQ VALUE

    instream_star: "*"
    instream_data: "DATA"
    dummy_param: "DUMMY"

    keyword_param: KEYWORD EQ (VALUE | JCL_QUOTED_STRING | list_val)
    list_val: "(" (VALUE | JCL_QUOTED_STRING | list_val) ("," (VALUE | JCL_QUOTED_STRING | list_val))* ")"

    OP_EXEC.4: "EXEC"
    OP_DD.4: "DD"
    EQ: "="
    PGM_KW.3: "PGM"
    PROC_KW.3: "PROC"
    PARM_KW.3: "PARM"
    DSN_KW.3: "DSN" | "DSNAME"
    DISP_KW.3: "DISP"
    SPACE_KW.3: "SPACE"
    DCB_KW.3: "DCB"
    VOL_KW.3: "VOL" | "VOLUME"
    SER_KW.3: "SER"
    LRECL_KW.3: "LRECL"
    RECFM_KW.3: "RECFM"
    BLKSIZE_KW.3: "BLKSIZE"
    DSORG_KW.3: "DSORG"
    EXEC_KW.3: "REGION" | "COND" | "TIME" | "DYNAMNBR" | "ADDRSPC"

    KEYWORD: "AMORG" | "AVGREC" | "BUFND" | "BUFNI" | "BUFSP" | "CHARS"
           | "COPIES" | "DATACLAS" | "DDNAME" | "DEST" | "DLM" | "DSNTYPE"
           | "EATTR" | "FILEDATA" | "FREE" | "LIKE" | "MAXGENS" | "MGMTCLAS"
           | "OUTLIM" | "PATHDISP" | "PATHMODE" | "PATHOPTS" | "REFDD"
           | "RETPD" | "RLS" | "STORCLAS" | "SUBSYS" | "SYMBOLS" | "SYMLIST" | "UNIT" | "SYSOUT"

    DISP_VAL: "NEW" | "OLD" | "SHR" | "MOD" | "DELETE" | "KEEP" | "PASS" | "CATLG" | "UNCATLG"
    SPACE_UNIT: "TRK" | "CYL" | NUMBER
    RECFM_VALUE: /[FUV][B]?[A-M]?/
    
    JCL_QUOTED_STRING: /'(?:[^']|'')*'/
    JCL_ID: /[A-Z#$@][A-Z0-9#$@]{0,7}/
    VALUE: /[A-Z0-9.#$@&*-+<>]+/

    %import common.NUMBER
    %import common.WS_INLINE
    %ignore WS_INLINE
"""

class JCLTransformer(Transformer):
    def JCL_ID(self, s): return str(s)
    def VALUE(self, s): return str(s)
    def NUMBER(self, n): return int(n)
    def RECFM_VALUE(self, s): return str(s)
    def JCL_QUOTED_STRING(self, s): return str(s).strip("'").replace("''", "'")
    
    def _merge_dicts(self, children):
        res = {}; [res.update(c) for c in children if isinstance(c, dict)]; return res
    
    def gdg_suffix(self, children): return "(" + "".join([str(c) for c in children if c is not None]) + ")"
    def dsn_value(self, children): return "".join([str(c) for c in children if c is not None])
    
    def exec_statement(self, children): return {"type": "EXEC", "label": children[0], "params": children[-1]}
    def unnamed_exec(self, children): return {"type": "EXEC", "label": None, "params": children[-1]}
    def dd_statement(self, children): return {"type": "DD", "label": children[0], "params": children[-1]}
    def unnamed_dd(self, children): return {"type": "DD", "label": None, "params": children[-1]}
    
    def exec_content(self, children): return self._merge_dicts(children)
    def exec_params(self, children): return self._merge_dicts(children)
    def dd_params(self, children): return self._merge_dicts(children)
    
    def pos_proc(self, children): return {"PROC": str(children[0])}
    def pgm_param(self, children): return {"PGM": str(children[-1])}
    def proc_param_kw(self, children): return {"PROC": str(children[-1])}
    def parm_param(self, children): return {"PARM": str(children[-1])}
    def dsn_param(self, children): return {"DSN": str(children[-1])}
    
    def disp_param(self, children):
        vals = [str(c) for c in children if str(c) not in ("DISP", "=", "(", ")", ",")]
        return {"DISP": vals}
    
    def lrecl_param(self, children): return {"LRECL": str(children[-1])}
    def recfm_param(self, children): return {"RECFM": str(children[-1])}
    def blksize_param(self, children): return {"BLKSIZE": str(children[-1])}
    def dsorg_param(self, children): return {"DSORG": str(children[-1])}
    
    def dcb_sublist(self, children): return self._merge_dicts(children)
    def dcb_param(self, children): return {"DCB": children[-1]}
    def space_quantities(self, children): return "".join([str(c) for c in children if c is not None])
    def space_param(self, children): return {"SPACE": "".join([str(c) for c in children if str(c) not in ("SPACE", "=")])}

    def instream_star(self, children): return {"INSTREAM": "*"}
    def instream_data(self, children): return {"INSTREAM": "DATA"}
    def dummy_param(self, children): return {"DUMMY": True}
    
    def exec_keyword(self, children): return {str(children[0]): str(children[-1])}
    def symbolic_override(self, children): return {str(children[0]): str(children[-1])}
    def keyword_param(self, children): return {str(children[0]): str(children[-1])}
    def list_val(self, children):
        items = [str(c) for c in children if str(c) not in ("(", ")", ",")]
        return items

class JCLParserManager:
    def __init__(self):
        self.parser = Lark(JCL_GRAMMAR, parser='lalr', transformer=JCLTransformer())
        self.steps = []
        self.current_step = None
    
    def process_results(self, results_list):
        proc_stack = []
        for stmt in results_list:
            if stmt.startswith("*PROC_START*"):
                m = re.search(r"label=(.*) proc=(.*)", stmt)
                if m: proc_stack.append({'label': m.group(1), 'proc': m.group(2)})
                continue
            if stmt.startswith("*PROC_END*"):
                if proc_stack: proc_stack.pop()
                continue
                
            if stmt.startswith("*PAYLOAD*"):
                if self.steps and self.steps[-1]['dds']:
                    last_dd = self.steps[-1]['dds'][-1]
                    if 'payload' not in last_dd: last_dd['payload'] = []
                    last_dd['payload'].append(stmt.replace("*PAYLOAD* ", ""))
                continue
            try:
                tree = self.parser.parse(stmt)
                if tree['type'] == 'EXEC':
                    self.current_step = {"step_info": tree, "dds": []}
                    # Context Application
                    if proc_stack:
                        outer = proc_stack[-1]
                        tree['final_proc_name'] = outer['proc']
                        tree['final_step_name'] = outer['label']
                        tree['final_proc_step_name'] = tree['label']
                    else:
                        tree['final_proc_name'] = tree['params'].get('PROC')
                        tree['final_step_name'] = tree['label']
                        tree['final_proc_step_name'] = None
                        
                    self.steps.append(self.current_step)
                elif tree['type'] == 'DD':
                    if self.current_step: self.current_step['dds'].append(tree)
            except Exception as e: 
                print(f"Parser Error: {stmt}\n{e}")
        return self.steps

# =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    def __init__(self, db_config, drop_tables=False):
        self.conn = psycopg2.connect(**db_config)
        self.create_tables(drop_tables)

    def create_tables(self, drop_tables):
        with self.conn.cursor() as cursor:
            if drop_tables:
                cursor.execute("DROP TABLE IF EXISTS DATA_ALLOCATIONS CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS STEPS CASCADE;")
                cursor.execute("DROP TABLE IF EXISTS PROJECTS CASCADE;")

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS PROJECTS (
                project_id SERIAL PRIMARY KEY,
                project_name TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS STEPS (
                project_id INTEGER NOT NULL REFERENCES PROJECTS(project_id),
                step_id INTEGER NOT NULL,
                relative_step VARCHAR(8) NOT NULL,
                step_name VARCHAR(8),
                proc_step_name VARCHAR(8),
                program_name VARCHAR(8),
                proc_name VARCHAR(8),
                parameters TEXT,
                cond_logic TEXT,
                PRIMARY KEY (project_id, step_id)
            );
            CREATE TABLE IF NOT EXISTS DATA_ALLOCATIONS (
                project_id INTEGER NOT NULL,
                step_id INTEGER NOT NULL,
                ds_id INTEGER NOT NULL,
                dd_name VARCHAR(8) NOT NULL,
                allocation_offset INTEGER NOT NULL DEFAULT 1,
                dsn VARCHAR(44),
                disp_status VARCHAR(8),
                disp_normal_term VARCHAR(8),
                disp_abnormal_term VARCHAR(8),
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
            CREATE INDEX IF NOT EXISTS idx_steps_project ON STEPS(project_id);
            CREATE INDEX IF NOT EXISTS idx_dd_step ON DATA_ALLOCATIONS(project_id, step_id);
            """)
        self.conn.commit()

    def insert_project_data(self, project_name, structured_data):
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO PROJECTS (project_name) VALUES (%s) 
                ON CONFLICT (project_name) DO NOTHING RETURNING project_id
            """, (project_name,))
            res = cursor.fetchone()
            project_id = res[0] if res else None
            if not project_id:
                cursor.execute("SELECT project_id FROM PROJECTS WHERE project_name = %s", (project_name,))
                project_id = cursor.fetchone()[0]

            relative_step_counter = 0
            cursor.execute("SELECT COALESCE(MAX(step_id), 0) FROM STEPS WHERE project_id = %s", (project_id,))
            step_id_counter = cursor.fetchone()[0]

            for step in structured_data:
                step_id_counter += 1; relative_step_counter += 1
                rel_step_str = f"X{relative_step_counter:07d}"
                info = step['step_info']; params = info['params']
                
                cursor.execute("""
                    INSERT INTO STEPS (project_id, step_id, relative_step, step_name, proc_step_name, program_name, proc_name, parameters, cond_logic)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    project_id, step_id_counter, rel_step_str, 
                    info['final_step_name'], info['final_proc_step_name'], 
                    params.get('PGM'), info['final_proc_name'],
                    params.get('PARM'), params.get('COND')
                ))

                last_dd_name, allocation_offset, ds_id_counter = None, 0, 0
                for dd in step['dds']:
                    ds_id_counter += 1
                    current_label = dd.get('label')
                    if current_label:
                        last_dd_name = current_label; allocation_offset = 1
                    else:
                        allocation_offset += 1

                    dd_params = dd['params']
                    dsn = dd_params.get('DSN')
                    if dd_params.get('DUMMY'): dsn = "(dummy)"
                    elif dd_params.get('INSTREAM'): dsn = "(input stream)"
                    elif 'SYSOUT' in dd_params: dsn = "(output stream)"
                    elif not dsn: dsn = "(work_ds)"

                    disp = dd_params.get('DISP', [])
                    status = disp[0] if len(disp) > 0 else None
                    normal = disp[1] if len(disp) > 1 else None
                    abnormal = disp[2] if len(disp) > 2 else None

                    lrecl, recfm, blksize = dd_params.get('LRECL'), dd_params.get('RECFM'), dd_params.get('BLKSIZE')
                    dcb_val, extra_dcb = dd_params.get('DCB'), {}
                    if isinstance(dcb_val, dict):
                        lrecl = lrecl or dcb_val.get('LRECL')
                        recfm = recfm or dcb_val.get('RECFM')
                        blksize = blksize or dcb_val.get('BLKSIZE')
                        extra_dcb = {k:v for k,v in dcb_val.items() if k not in ('LRECL','RECFM','BLKSIZE')}

                    cursor.execute("""
                        INSERT INTO DATA_ALLOCATIONS (
                            project_id, step_id, ds_id, dd_name, allocation_offset, dsn, disp_status, 
                            disp_normal_term, disp_abnormal_term, unit, vol_ser, is_dummy, 
                            instream_ref, lrecl, blksize, recfm, dcb_attributes
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        project_id, step_id_counter, ds_id_counter, last_dd_name, allocation_offset, dsn,
                        status, normal, abnormal, dd_params.get('UNIT'), dd_params.get('VOL'),
                        bool(dd_params.get('DUMMY')), "\n".join(dd.get('payload', [])),
                        lrecl, blksize, recfm, Json(extra_dcb)
                    ))
        self.conn.commit()
        print(f"Extraction successful for '{project_name}'.")

# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

if __name__ == "__main__":
    CONFIG_FILE = 'config.json'
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        db_credentials = {
            "host": "localhost", "database": config.get("DATABASE", "jcl_db"),
            "user": config.get("USER", "postgres"), "password": config.get("PASSWORD", ""), "port": 5432
        }
        pre = JCLPreprocessor(config)
        path = pre.resolve_path(config["FILE"])
        if path:
            res = pre.preprocess_file(path)
            manager = JCLParserManager()
            data = manager.process_results(res)
            db = DatabaseManager(db_credentials, drop_tables=config.get("DROP_TABLES", False))
            db.insert_project_data(config["PROJECT"], data)