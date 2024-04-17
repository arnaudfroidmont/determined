CREATE TABLE public.namespaces (
    id integer NOT NULL,
    name TEXT NOT NULL,
    resource_manager_name text NOT NULL,
    workspace_id INT REFERENCES workspaces(id)
);
