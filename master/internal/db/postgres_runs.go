package db

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/determined-ai/determined/master/pkg/model"
)

// GetRunMetadataKeys returns the unique metadata keys for a run.
func GetRunMetadataKeys(ctx context.Context, rID int) ([]string, error) {
	var res []string
	err := Bun().NewSelect().Model(&res).Table("runs_metadata_index").
		Distinct().
		Column("flat_key").
		Where("run_id = ?", rID).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying run metadata indexes: %w", err)
	}
	return res, nil
}

// UpdateRunMetadata updates the metadata of a run, including the metadata indexes.
func UpdateRunMetadata(
	ctx context.Context,
	rID int,
	metadata model.RunMetadata,
	metadataIndexes []model.RunMetadataIndex,
) error {
	err := Bun().RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := Bun().NewUpdate().Table("runs").Set("metadata = ?", metadata).Where("id = ?", rID).Exec(ctx)
		if err != nil {
			return fmt.Errorf("updating run metadata: %w", err)
		}
		_, err = Bun().NewInsert().Model(&metadataIndexes).Exec(ctx)
		if err != nil {
			return fmt.Errorf("inserting run metadata indexes: %w", err)
		}
		return nil
	})
	return err
}

// GetRun returns a run by its ID.
func GetRun(ctx context.Context, rID int) (*model.Run, error) {
	r := &model.Run{ID: rID}
	if err := Bun().NewSelect().Model(r).WherePK().Scan(ctx); err != nil {
		return nil, fmt.Errorf("querying run: %w", err)
	}
	return r, nil
}
