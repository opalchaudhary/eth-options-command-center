create extension if not exists pgcrypto;

create table if not exists public.deltaforge_users (
    id uuid primary key default gen_random_uuid(),
    username text not null unique,
    password_hash text not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    last_login_at timestamptz
);

create table if not exists public.deltaforge_sessions (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null references public.deltaforge_users(id) on delete cascade,
    session_token_hash text not null unique,
    created_at timestamptz not null default now(),
    expires_at timestamptz not null,
    revoked_at timestamptz
);

create table if not exists public.deltaforge_login_attempts (
    id uuid primary key default gen_random_uuid(),
    username text not null,
    ip_address text,
    success boolean not null default false,
    failure_reason text,
    created_at timestamptz not null default now()
);

create index if not exists idx_deltaforge_sessions_token_hash
    on public.deltaforge_sessions(session_token_hash);

create index if not exists idx_deltaforge_sessions_user_expires
    on public.deltaforge_sessions(user_id, expires_at desc);

create index if not exists idx_deltaforge_login_attempts_username_created
    on public.deltaforge_login_attempts(username, created_at desc);

