package settings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/jackc/pgx"
	"github.com/jpillora/chisel/share/cio"
)

type Users struct {
	sync.RWMutex
	inner map[string]*User
}

func NewUsers() *Users {
	return &Users{inner: map[string]*User{}}
}

// Len returns the numbers of users
func (u *Users) Len() int {
	u.RLock()
	l := len(u.inner)
	u.RUnlock()
	return l
}

// Get user from the index by key
func (u *Users) Get(key string) (*User, bool) {
	u.RLock()
	user, found := u.inner[key]
	u.RUnlock()
	return user, found
}

// Set a users into the list by specific key
func (u *Users) Set(key string, user *User) {
	u.Lock()
	u.inner[key] = user
	u.Unlock()
}

// Del ete a users from the list
func (u *Users) Del(key string) {
	u.Lock()
	delete(u.inner, key)
	u.Unlock()
}

// AddUser adds a users to the set
func (u *Users) AddUser(user *User) {
	u.Set(user.Name, user)
}

// Reset all users to the given set,
// Use nil to remove all.
func (u *Users) Reset(users []*User) {
	m := map[string]*User{}
	for _, u := range users {
		m[u.Name] = u
	}
	u.Lock()
	u.inner = m
	u.Unlock()
}

// UserIndex is a reloadable user source
type UserIndex struct {
	*cio.Logger
	*Users
	configFile string
}

// NewUserIndex creates a source for users
func NewUserIndex(logger *cio.Logger) *UserIndex {
	return &UserIndex{
		Logger: logger.Fork("users"),
		Users:  NewUsers(),
	}
}

// LoadUsers is responsible for loading users from a file
func (u *UserIndex) LoadUsers(configFile string) error {
	u.configFile = configFile
	u.Infof("Loading configuration file %s", configFile)
	if err := u.loadUserIndex(); err != nil {
		return err
	}
	if err := u.addWatchEvents(); err != nil {
		return err
	}
	return nil
}

func columnExists(conn *pgx.Conn, tableName, columnName string) bool {
	var exists bool
	query := `SELECT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name=$1 AND column_name=$2
    )`
	err := conn.QueryRow(query, tableName, columnName).Scan(&exists)
	if err != nil {
		log.Fatalf("Error checking if column exists: %v\n", err)
		return false
	}
	return exists
}

func updateUsersData(u *UserIndex, conn *pgx.Conn, defaultTableName string) error {

	addressesExists := columnExists(conn, defaultTableName, "addresses")

	var query string
	if addressesExists {
		query = fmt.Sprintf("SELECT username, password, addresses FROM %s", defaultTableName)
	} else {
		query = fmt.Sprintf("SELECT username, password FROM %s", defaultTableName)
	}

	rows, err := conn.Query(query)
	if err != nil {
		log.Fatalf("Error in getting users data. Error: %v\n", err)
		return err
	}
	defer rows.Close()

	var users []*User
	for rows.Next() {
		var username, password string
		var addresses []string

		user := &User{}
		if addressesExists {
			if err := rows.Scan(&username, &password, &addresses); err != nil {
				log.Fatalf("Error in scanning authentication data from database. Error: %v\n", err)
				return err
			}

			for _, addr := range addresses {
				if addr == "" || addr == "*" {
					user.Addrs = append(user.Addrs, UserAllowAll)
				} else {
					re, err := regexp.Compile(addr)
					if err != nil {
						log.Fatalf("Invalid address regex")
						return err
					}
					user.Addrs = append(user.Addrs, re)
				}
			}

		} else {
			if err := rows.Scan(&username, &password); err != nil {
				log.Fatalf("Error in scanning authentication data from database. Error: %v\n", err)
				return err
			}
			user.Addrs = append(user.Addrs, UserAllowAll)
		}

		user.Name = username
		user.Pass = password
		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error: %v\n", err)
		return err
	}

	u.Reset(users)
	return nil
}

func (u *UserIndex) LoadUsersFromDatabase(connString string, tableName *string) error {
	defaultTableName := "users"

	if tableName != nil {
		match, err := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_]*$`, *tableName)
		if err != nil || !match {
			log.Fatalf("Invalid table name. Error: %v\n", err)
			return fmt.Errorf("invalid table name: %s", defaultTableName)
		}
		defaultTableName = *tableName
	}

	connConfig, err := pgx.ParseURI(connString)
	if err != nil {
		log.Fatalf("Error in Parsing the URI. Error: %v\n", err)
		return err
	}

	conn, err := pgx.Connect(connConfig)
	go listenForChanges(u, connString, defaultTableName)
	if err != nil {
		log.Fatalf("Error in connecting to database. Error: %v\n", err)
		return err
	}
	defer conn.Close()
	return updateUsersData(u, conn, defaultTableName)
}

func listenForChanges(u *UserIndex, connString string, defaultTableName string) {

	connConfig, err := pgx.ParseURI(connString)
	if err != nil {
		log.Fatalf("Error in parsing authentication DB URI. Error: %v\n", err)
		return
	}

	conn, err := pgx.Connect(connConfig)
	if err != nil {
		log.Fatalf("Error in connecting the database. Error: %v\n", err)
		return
	}
	defer conn.Close()

	_, err = conn.Exec("LISTEN users_data_updates")
	if err != nil {
		log.Fatalf("Unable to execute LISTEN command: %v\n", err)
	}

	log.Println("Listening for user updates...")

	for {
		notification, err := conn.WaitForNotification(context.Background())
		if err != nil {
			log.Fatalf("Error waiting for notification: %v\n", err)
		}

		log.Printf("Received notification successfully: %s\n", notification.Payload)
		err = updateUsersData(u, conn, defaultTableName)
		if err != nil {
			log.Fatalf("Error in fetching the user data: %v\n", err)
		}
	}
}

// watchEvents is responsible for watching for updates to the file and reloading
func (u *UserIndex) addWatchEvents() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err := watcher.Add(u.configFile); err != nil {
		return err
	}
	go func() {
		for e := range watcher.Events {
			if e.Op&fsnotify.Write != fsnotify.Write {
				continue
			}
			if err := u.loadUserIndex(); err != nil {
				u.Infof("Failed to reload the users configuration: %s", err)
			} else {
				u.Debugf("Users configuration successfully reloaded from: %s", u.configFile)
			}
		}
	}()
	return nil
}

// loadUserIndex is responsible for loading the users configuration
func (u *UserIndex) loadUserIndex() error {
	if u.configFile == "" {
		return errors.New("configuration file not set")
	}
	b, err := os.ReadFile(u.configFile)
	if err != nil {
		return fmt.Errorf("Failed to read auth file: %s, error: %s", u.configFile, err)
	}
	var raw map[string][]string
	if err := json.Unmarshal(b, &raw); err != nil {
		return errors.New("Invalid JSON: " + err.Error())
	}
	users := []*User{}
	for auth, remotes := range raw {
		user := &User{}
		user.Name, user.Pass = ParseAuth(auth)
		if user.Name == "" {
			return errors.New("Invalid user:pass string")
		}
		for _, r := range remotes {
			if r == "" || r == "*" {
				user.Addrs = append(user.Addrs, UserAllowAll)
			} else {
				re, err := regexp.Compile(r)
				if err != nil {
					return errors.New("Invalid address regex")
				}
				user.Addrs = append(user.Addrs, re)
			}
		}
		users = append(users, user)
	}
	//swap
	u.Reset(users)
	return nil
}
