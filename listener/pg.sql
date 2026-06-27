--
-- PostgreSQL database dump
--

\restrict Y9pzVcdfMJk0edZLLEIrueAQiq85zk9PLGVAF4jJ9XfTUeFHBn1a63bzqZmfCNB

-- Dumped from database version 18.1
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET row_security = off;

--
-- Name: refs; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.refs AS ENUM (
    'cat.accounts',
    'cat.abonents',
    'cat.branches',
    'cat.characteristics',
    'cat.divisions',
    'cat.leads',
    'cat.partners',
    'cat.planning_keys',
    'cat.products',
    'cat.projects',
    'cat.servers',
    'cat.specifications',
    'cat.users',
    'doc.calc_order',
    'doc.planning_event',
    'doc.work_centers_task',
    'doc.work_centers_performance',
    'doc.purchase_order',
    'doc.debit_cash_order',
    'doc.credit_cash_order',
    'doc.credit_card_order',
    'doc.debit_bank_order',
    'doc.credit_bank_order',
    'doc.selling',
    'doc.purchase',
    'doc.nom_prices_setup',
    'doc.inventory_cuts',
    'doc.inventory_goods',
    'doc.scaning',
    'unknown',
    'design'
);


--
-- Name: append(integer, integer, integer, public.refs, uuid, character varying, boolean, uuid, uuid, timestamp without time zone); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.append(IN year integer, IN abonent integer, IN branch integer, IN ptype public.refs, IN pref uuid, IN prev character varying, IN deleted boolean, IN partner uuid, IN department uuid, IN date timestamp without time zone)
    LANGUAGE plpgsql
    AS $$
BEGIN
	if not exists (SELECT 1 FROM feed  WHERE type=ptype and ref=pref and rev=prev) then
insert into feed (year, abonent, branch, type, ref, rev, deleted, partner, department, date)
    VALUES (year, abonent, branch, ptype, pref, prev, deleted, partner, department, date);
	end if;
END;
$$;


SET default_table_access_method = heap;

--
-- Name: feed; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.feed (
    seq uuid DEFAULT uuidv7() NOT NULL,
    year integer,
    abonent integer,
    branch integer,
    type public.refs,
    ref uuid,
    rev character varying(64),
    deleted boolean,
    partner uuid,
    department uuid,
    date timestamp with time zone
);


--
-- Name: servers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.servers (
    year integer NOT NULL,
    abonent integer NOT NULL,
    branch integer NOT NULL,
    addr character varying(100)
);


--
-- Name: settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.settings (
    param character varying(100) NOT NULL,
    value jsonb NOT NULL
);


--
-- Name: feed_type_ref_rev_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX feed_type_ref_rev_idx ON public.feed USING btree (type, ref, rev DESC) WITH (deduplicate_items='true');


--
-- Name: servers servers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.servers
    ADD CONSTRAINT servers_pkey PRIMARY KEY (year, abonent, branch);


--
-- Name: feed feed_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.feed
    ADD CONSTRAINT feed_pkey PRIMARY KEY (seq);


--
-- Name: settings settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.settings
    ADD CONSTRAINT settings_pkey PRIMARY KEY (param);



