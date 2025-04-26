create table dictionary_relations
(
    id          integer generated always as identity
        constraint dictionary_relations_pk
            primary key,
    id_master   integer
        constraint dictionary_relations_dictionary_positions_id_fk
            references dictionary_positions,
    id_slave    integer
        constraint dictionary_relations_dictionary_positions_id_fk_2
            references dictionary_positions,
    start_date  date,
    finish_date date
);

alter table dictionary_relations
    owner to admin_eisgs;

