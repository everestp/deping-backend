package repositories

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type userRepo struct {
	pool *pgxpool.Pool
}

func (r *userRepo) Create(ctx context.Context, email, passwordHash, walletPubkey string) (*User, error) {
	const q = `
		INSERT INTO users (email, password_hash, wallet_pubkey)
		VALUES ($1, $2, $3)
		RETURNING id, email, password_hash, wallet_pubkey, created_at`

	u := &User{}
	err := r.pool.QueryRow(ctx, q, email, passwordHash, walletPubkey).
		Scan(&u.ID, &u.Email, &u.PasswordHash, &u.WalletPubkey, &u.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("userRepo.Create: %w", err)
	}
	return u, nil
}

func (r *userRepo) FindByEmail(ctx context.Context, email string) (*User, error) {
	const q = `
		SELECT id, email, password_hash, wallet_pubkey, created_at
		FROM users
		WHERE email = $1 AND deleted_at IS NULL`

	u := &User{}
	err := r.pool.QueryRow(ctx, q, email).
		Scan(&u.ID, &u.Email, &u.PasswordHash, &u.WalletPubkey, &u.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("userRepo.FindByEmail: %w", err)
	}
	return u, nil
}

func (r *userRepo) FindByWallet(ctx context.Context, walletPubkey string) (*User, error) {
	const q = `
		SELECT id, email, password_hash, wallet_pubkey, created_at
		FROM users
		WHERE wallet_pubkey = $1 AND deleted_at IS NULL`

	u := &User{}
	err := r.pool.QueryRow(ctx, q, walletPubkey).
		Scan(&u.ID, &u.Email, &u.PasswordHash, &u.WalletPubkey, &u.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("userRepo.FindByWallet: %w", err)
	}
	return u, nil
}
