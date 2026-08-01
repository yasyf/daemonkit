// Package durable makes filesystem state survive crashes: atomic, fsynced
// publication of files and directory mutations, a strict validated JSON codec,
// and one bounded cross-process lock. Every function is durable — a caller
// that does not need power-loss durability does not need this package.
package durable
