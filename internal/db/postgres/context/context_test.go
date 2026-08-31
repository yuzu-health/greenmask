package context

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	contextTestDb = `
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE users
(
    user_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL
);

CREATE TABLE orders
(
    order_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID REFERENCES users (user_id),
    order_date DATE NOT NULL
);


CREATE TABLE public."foo"
(
    id      uuid                   NOT NULL default uuid_generate_v4(),
    locale  text                   NOT NULL,
    subject character varying(100) NOT NULL,
    body    text                   NOT NULL
);

INSERT INTO users (username)
VALUES ('john_doe');
INSERT INTO users (username)
VALUES ('jane_smith');

INSERT INTO orders (user_id, order_date)
VALUES ((SELECT user_id FROM users WHERE username = 'john_doe'), '2024-10-31'),
       ((SELECT user_id FROM users WHERE username = 'jane_smith'), '2024-10-30');

insert into public."foo" (locale, subject, body)
values ('da-DK', 'Opsæt din konto hos {{coachName}}',
        ' Hej {{firstName}},\n\nSå er vi klar til at tage første skridt.\nTryk på følgende link for at få adgang til din side:\n[{{{accessLink}}}]({{{accessLink}}})\n\nMed venlig hilsen\n{{coachName}}'),
       ('da-DK', 'Kvittering på {{productName}}',
        'Hej {{firstName}},\n\nHermed en kvittering på din bestilling af {{productName}}\n\n- Total beløb: {{paymentAmount}}\n- Dato for køb: {{paymentDate}}\n\nBeløbet vil ikke blive trukket på din konto før den er fremsendt til dig.\n\nInden for et par dage vil jeg sende det din vej på samme e-mail, som du har modtaget denne kvittering på.\n\nRigtig god dag\n{{coachName}}'),
       ('en-US', 'Kvittering på {{productName}}',
        'Hej {{firstName}},\n\nHermed en kvittering på din bestilling af {{productName}}\n\n- Total beløb: {{paymentAmount}}\n- Dato for køb: {{paymentDate}}\n\nBeløbet vil ikke blive trukket på din konto før den er fremsendt til dig.\n\nInden for et par dage vil jeg sende det din vej på samme e-mail, som du har modtaget denne kvittering på.\n\nRigtig god dag\n{{coachName}}'),
       ('sv-SE', 'Kvittering på {{productName}}',
        'Hej {{firstName}},\n\nHermed en kvittering på din bestilling af {{productName}}\n\n- Total beløb: {{paymentAmount}}\n- Dato for køb: {{paymentDate}}\n\nBeløbet vil ikke blive trukket på din konto før den er fremsendt til dig.\n\nInden for et par dage vil jeg sende det din vej på samme e-mail, som du har modtaget denne kvittering på.\n\nRigtig god dag\n{{coachName}}'),
       ('fr-FR', 'Reçu de paiement pour le {{productName}}',
        'Bonjour {{firstName}},\n\nCeci est le reçu de paiement pour votre commande du  {{productName}}\n\n- Montant total: {{paymentAmount}}\n- Date d''achat: {{paymentDate}}\n\nVous ne serez pas facturé avant que le produit ne vous soit envoyé.\n\nDans quelques jours je vous l''enverrai sur cette adresse e-mail.\n\n\nBien cordialement\n{{coachName}}'),
       ('de-DE', 'Bestellung eines {{productName}}',
        'Hallo {{firstName}},\n\nAnbei findest du die Übersicht deiner Bestellung über einen {{productName}}\n\n- Gesamtbetrag: {{paymentAmount}}\n- Datum der Bestellung: {{paymentDate}}\n\nDir wird nichts in Rechnung gestellt, bevor du deinen Plan erhalten hast.\nIch werde dir in wenigen Tagen eine Bestätigung über diese E-Mail Adresse zukommen lassen. \n\nBeste Grüße, \n{{coachName}}'),
       ('es-MX', 'Recibo de {{productName}}',
        'Hola, {{firstName}}, \n\nEste es el recibo de tu pedido de un {{productName}} \n\n- Importe total: {{paymentAmount}} \n- Fecha de compra: {{paymentDate}} \n\nNo se te realizará el cargo hasta que se te envíe el producto.\n\nLo recibirás dentro de pocos días en este mismo correo electrónico. \n\nSaludos, \n{{coachName}}\n'),
       ('da-DK', 'Din kostplan',
        'Hej {{firstName}},\n\nDin kostplan er nu sammensat - du finder den vedhæftet som pdf.\n\nDu kan downloade og printe den, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå den via din smartphone, så du altid har den ved din side.\n\nJeg håber, at du bliver rigtig glad for planen. Følger du den, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte en kostplan hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('en-US', 'Din kostplan',
        'Hej {{firstName}},\n\nDin kostplan er nu sammensat - du finder den vedhæftet som pdf.\n\nDu kan downloade og printe den, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå den via din smartphone, så du altid har den ved din side.\n\nJeg håber, at du bliver rigtig glad for planen. Følger du den, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte en kostplan hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('sv-SE', 'Din kostplan',
        'Hej {{firstName}},\n\nDin kostplan er nu sammensat - du finder den vedhæftet som pdf.\n\nDu kan downloade og printe den, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå den via din smartphone, så du altid har den ved din side.\n\nJeg håber, at du bliver rigtig glad for planen. Følger du den, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte en kostplan hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('fr-FR', 'Votre programme alimentaire',
        'Bonjour {{firstName}},\n\nVotre programme alimentaire est maintenant prêt - il est joint à cet email en format PDF.\nVous pouvez le télécharger et l''imprimer puis l''accrocher facilement à votre réfrigérateur.\nVous pouvez également y accéder directement depuis votre portable.\n\nJ''espère vraiment que vous apprécierez de suivre ce plan. Si vous vous y tenez, je suis sûr que vous obtiendrez d''excellents résultats.\n\nMerci d''avoir passé votre commande.\n\nBien cordialement\n{{coachName}}\n'),
       ('de-DE', 'Dein Ernährungsplan',
        'Hallo {{firstName}},\n\nDein Ernährungsplan ist fertig und steht für dich bereit. Du findest ihn in der angehängten PDF. Du kannst das Dokument einfach herunterladen und ausdrucken, oder über dein Smartphone darauf zugreifen.\n\nIch hoffe sehr, dass du den Ernährungsplan genießt. Wenn du dich so gut es geht an den Plan hältst, bin ich zuversichtlich, dass du großartige Ergebnisse erzielen wirst\n\n\nVielen Dank für deine Bestellung.\n\nBeste Grüße, \n{{coachName}}'),
       ('es-MX', 'Tu plan alimenticio',
        'Hola, {{firstName}}, \n\nTu plan alimenticio está listo; está adjunto como PDF. Puedes descargarlo e imprimirlo y colgarlo fácilmente en tu refrigerador. También puedes acceder a él directamente desde tu smartphone. \n\nEspero que te guste el plan alimenticio. Si lo sigues, sin duda conseguirás grandes resultados. \n\nGracias por hacer tu pedido. \n\nSaludos, \n{{coachName}}'),
       ('da-DK', 'Dit træningsprogram',
        'Hej {{firstName}},\n\nDit træningsprogram er nu sammensat - du finder det vedhæftet som pdf.\n\nDu kan downloade og printe det, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå det via din smartphone, så du altid har det ved din side.\n\nJeg håber, at du bliver rigtig glad for programmet. Følger du det, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte et træningsprogram hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('en-US', 'Dit træningsprogram',
        'Hej {{firstName}},\n\nDit træningsprogram er nu sammensat - du finder det vedhæftet som pdf.\n\nDu kan downloade og printe det, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå det via din smartphone, så du altid har det ved din side.\n\nJeg håber, at du bliver rigtig glad for programmet. Følger du det, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte et træningsprogram hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('sv-SE', 'Dit træningsprogram',
        'Hej {{firstName}},\n\nDit træningsprogram er nu sammensat - du finder det vedhæftet som pdf.\n\nDu kan downloade og printe det, lige til at hænge på køleskabet.\nDu kan selvfølgelig også tilgå det via din smartphone, så du altid har det ved din side.\n\nJeg håber, at du bliver rigtig glad for programmet. Følger du det, er jeg sikker på, du vil opleve gode resultater.\nIgen tak fordi du bestilte et træningsprogram hos mig.\n\nMed venlig hilsen\n{{coachName}}'),
       ('fr-FR', 'Votre programme d''entraînement',
        'Bonjour {{firstName}},\n\nVotre programme d''entraînement est maintenant prêt - il est joint à cet email en format PDF.\nVous pouvez le télécharger et l''imprimer afin de le transporter avec vous.\nVous pouvez également y accéder directement depuis votre portable.\n\nJ''espère vraiment que vous apprécierez de suivre ce plan. Si vous vous y tenez, je suis sûr que vous obtiendrez d''excellents résultats.\n\nMerci d''avoir passé votre commande.\n\nBien cordialement\n{{coachName}})');
`
)

const multiCycleGroupSccDb = `
CREATE TABLE org (
    id            INT PRIMARY KEY,
    name          TEXT NOT NULL,
    aggregator_id INT
);

CREATE TABLE connect_account (
    id     INT PRIMARY KEY,
    org_id INT
);

CREATE TABLE financial_account (
    id                  INT PRIMARY KEY,
    external_account_id INT NOT NULL
);

CREATE TABLE aggregator (
    id                   INT PRIMARY KEY,
    org_id               INT,
    financial_account_id INT NOT NULL,
    captive_id           INT
);

ALTER TABLE org ADD CONSTRAINT org_aggregator_fkey
    FOREIGN KEY (aggregator_id) REFERENCES aggregator (id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE connect_account ADD CONSTRAINT connect_account_org_fkey
    FOREIGN KEY (org_id) REFERENCES org (id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE financial_account ADD CONSTRAINT financial_account_connect_fkey
    FOREIGN KEY (external_account_id) REFERENCES connect_account (id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE aggregator ADD CONSTRAINT aggregator_org_fkey
    FOREIGN KEY (org_id) REFERENCES org (id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE aggregator ADD CONSTRAINT aggregator_financial_account_fkey
    FOREIGN KEY (financial_account_id) REFERENCES financial_account (id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE aggregator ADD CONSTRAINT aggregator_captive_fkey
    FOREIGN KEY (captive_id) REFERENCES aggregator (id) DEFERRABLE INITIALLY DEFERRED;

BEGIN;
SET CONSTRAINTS ALL DEFERRED;

INSERT INTO org (id, name, aggregator_id)
VALUES (1, 'in-scope-a', 101), (2, 'in-scope-b', 102), (3, 'out-a', 103), (4, 'out-b', NULL);

INSERT INTO connect_account (id, org_id)
VALUES (201, 1), (202, 2), (203, 3), (204, NULL);

INSERT INTO financial_account (id, external_account_id)
VALUES (301, 201), (302, 202), (303, 203), (304, 204);

INSERT INTO aggregator (id, org_id, financial_account_id, captive_id)
VALUES (101, 1, 301, NULL), (102, 2, 302, 101), (103, 3, 303, NULL), (104, NULL, 304, NULL);

COMMIT;
`

// getSubsetQueryResultIds - executes the subset query of each table and returns the ordered ids per table name
func getSubsetQueryResultIds(
	ctx context.Context, t *testing.T, tx pgx.Tx, dataSectionObjects []entries.Entry,
) map[string][]int {
	res := make(map[string][]int)
	for _, table := range dataSectionObjects {
		tab, ok := table.(*entries.Table)
		if !ok {
			continue
		}
		require.NotEmptyf(t, tab.Query, "Table %s", tab.Name)
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT id FROM (%s) AS s", tab.Query))
		require.NoErrorf(t, err, "Table %s", tab.Name)
		ids := []int{}
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		rows.Close()
		require.NoError(t, rows.Err())
		slices.Sort(ids)
		res[tab.Name] = ids
	}
	return res
}

func TestNewRuntimeContext_subset_multiCycleGroupsInScc(t *testing.T) {
	// This test is a regression test for https://github.com/GreenmaskIO/greenmask/issues/197
	// The SCC contains three cycle groups: the aggregator self-reference, the cycle
	// {aggregator, org} and the cycle {aggregator, financial_account, connect_account, org}.
	// Previously the subset query generation panicked with
	// "IMPLEMENT ME: more than one cycle group found in SCC".
	ctx := context.Background()
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup()

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, multiCycleGroupSccDb))
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "org",
				SubsetConds: []string{
					"public.org.id IN (1, 2)",
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())

	expectedIds := map[string][]int{
		"org":               {1, 2},
		"aggregator":        {101, 102},
		"connect_account":   {201, 202},
		"financial_account": {301, 302},
	}
	require.Equal(t, expectedIds, getSubsetQueryResultIds(ctx, t, tx, rc.DataSectionObjects))
}

func TestNewRuntimeContext_subset_selfReferenceAndCycle(t *testing.T) {
	// This test is a regression test for https://github.com/GreenmaskIO/greenmask/issues/197
	// The SCC contains two cycle groups: the aggregator self-reference and the cycle
	// {aggregator, org}.
	ctx := context.Background()
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup()

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, multiCycleGroupSccDb))
	_, err = con.Exec(ctx, "ALTER TABLE aggregator DROP CONSTRAINT aggregator_financial_account_fkey")
	require.NoError(t, err)
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "org",
				SubsetConds: []string{
					"public.org.id IN (1, 2)",
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())

	expectedIds := map[string][]int{
		"org":               {1, 2},
		"aggregator":        {101, 102},
		"connect_account":   {201, 202, 204},
		"financial_account": {301, 302, 304},
	}
	require.Equal(t, expectedIds, getSubsetQueryResultIds(ctx, t, tx, rc.DataSectionObjects))
}

func TestNewRuntimeContext_subset_singleCycleGroup(t *testing.T) {
	// This test validates that the single cycle group generation keeps working: the SCC contains
	// only the cycle {aggregator, org} after the self-reference and the aggregator -> financial_account
	// edges are dropped
	ctx := context.Background()
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup()

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, multiCycleGroupSccDb))
	_, err = con.Exec(ctx, "ALTER TABLE aggregator DROP CONSTRAINT aggregator_financial_account_fkey")
	require.NoError(t, err)
	_, err = con.Exec(ctx, "ALTER TABLE aggregator DROP CONSTRAINT aggregator_captive_fkey")
	require.NoError(t, err)
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "org",
				SubsetConds: []string{
					"public.org.id IN (1, 2)",
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())

	expectedIds := map[string][]int{
		"org":               {1, 2},
		"aggregator":        {101, 102},
		"connect_account":   {201, 202, 204},
		"financial_account": {301, 302, 304},
	}
	require.Equal(t, expectedIds, getSubsetQueryResultIds(ctx, t, tx, rc.DataSectionObjects))
}

func TestNewRuntimeContext(t *testing.T) {
	ctx := context.Background()
	// Start the PostgreSQL container
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup() // Ensure the container is terminated after the test

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, contextTestDb))
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "users",
				Transformers: []*domains.TransformerConfig{
					{
						Name:               transformers.RandomUuidTransformerName,
						ApplyForReferences: true,
						Params: toolkit.StaticParameters{
							"column": toolkit.ParamsValue("user_id"),
							"engine": toolkit.ParamsValue("hash"),
						},
					},
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())
}

func TestNewRuntimeContext_regression_244(t *testing.T) {
	// This test is a regression test for https://github.com/GreenmaskIO/greenmask/issues/244
	// The problem was that the table graph used a shared tables list, which was later sorted by the table size scoring
	// function. This means that the graph tables must not be sorted by the size scoring function.
	ctx := context.Background()
	// Start the PostgreSQL container
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup() // Ensure the container is terminated after the test

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, contextTestDb))
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "users",
				Transformers: []*domains.TransformerConfig{
					{
						Name:               transformers.RandomUuidTransformerName,
						ApplyForReferences: true,
						Params: toolkit.StaticParameters{
							"column": toolkit.ParamsValue("user_id"),
							"engine": toolkit.ParamsValue("hash"),
						},
					},
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())

	// Check that tables are sorted by oid in graph as defined in buildTableSearchQuery function
	tablesOids := make([]toolkit.Oid, 0, len(rc.DataSectionObjects))
	for _, table := range rc.DataSectionObjects {
		tab, ok := table.(*entries.Table)
		if !ok {
			continue
		}
		tablesOids = append(tablesOids, tab.Oid)
	}
	slices.Sort(tablesOids)

	for posInContext, table := range rc.Graph.GetTables() {
		expectedPos := slices.Index(tablesOids, table.Oid)
		assert.Equalf(t, expectedPos, posInContext,
			"Expected table %s to be at position %d, but it is at %d", table.Name, expectedPos, posInContext,
		)
	}

	// Validate inherited transformers
	expectedTablesWithTransformer := map[string]int{
		"users":  1,
		"orders": 1,
		"foo":    0,
	}

	for _, table := range rc.DataSectionObjects {
		tab, ok := table.(*entries.Table)
		if !ok {
			continue
		}
		if _, ok := expectedTablesWithTransformer[tab.Name]; ok {
			assert.Equalf(t, expectedTablesWithTransformer[tab.Name], len(tab.TransformersContext), "Table %s", tab.Name)
		} else {
			assert.Empty(t, tab.TransformersContext, "Table %s", tab.Name)
		}
	}
}

func TestNewRuntimeContext_regression_247(t *testing.T) {
	// This test is a regression test for https://github.com/GreenmaskIO/greenmask/issues/247
	// It validates that subset conditions are correctly applied to the query
	ctx := context.Background()
	// Start the PostgreSQL container
	connStr, cleanup, err := runPostgresContainer(ctx)
	require.NoError(t, err)
	defer cleanup() // Ensure the container is terminated after the test

	con, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer con.Close(ctx) // nolint: errcheck
	require.NoError(t, initTables(ctx, con, contextTestDb))
	tx, err := con.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx) // nolint: errcheck
	cfg := &domains.Dump{
		Transformation: []*domains.Table{
			{
				Schema: "public",
				Name:   "users",
				SubsetConds: []string{
					"public.users.user_id = '62c8c546-2420-4ca6-9961-d2cce26f7cb2'",
				},
			},
		},
	}
	rc, err := NewRuntimeContext(ctx, tx, cfg, utils.DefaultTransformerRegistry, nil, testContainerPgVersion*10000)
	require.NoError(t, err)
	require.NotNil(t, rc)
	require.False(t, rc.IsFatal())

	expectedTablesWithSubsetQuery := map[string]string{
		"users":  "SELECT \"public\".\"users\".\"user_id\", \"public\".\"users\".\"username\" FROM \"public\".\"users\"   WHERE ( ( public.users.user_id = '62c8c546-2420-4ca6-9961-d2cce26f7cb2' ) )",
		"orders": "SELECT \"public\".\"orders\".\"order_id\", \"public\".\"orders\".\"user_id\", \"public\".\"orders\".\"order_date\" FROM \"public\".\"orders\"  LEFT JOIN \"public\".\"users\" ON \"public\".\"orders\".\"user_id\" = \"public\".\"users\".\"user_id\" AND ( public.users.user_id = '62c8c546-2420-4ca6-9961-d2cce26f7cb2' ) WHERE ( ((\"public\".\"orders\".\"user_id\" IS NULL OR \"public\".\"users\".\"user_id\" IS NOT NULL)) )",
		"foo":    "",
	}

	for _, table := range rc.DataSectionObjects {
		tab, ok := table.(*entries.Table)
		if !ok {
			continue
		}
		assert.Equalf(t, expectedTablesWithSubsetQuery[tab.Name], tab.Query, "Table %s", tab.Name)
	}
}
