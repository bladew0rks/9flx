# 9flx

9flx turns your Fluxer account into a 9P filesystem. Everything becomes a file: read a conversation with `cat history`, or send a message with `echo 'hello' > send`.

## Why?

A counter-question: Why not?

## Run

Go 1.24 or newer is required.

```sh
go build -o 9flx ./cmd/9flx
install -m 600 /dev/null "$HOME/.config/fluxer-token"
printf '%s' "$YOUR_FLUXER_SESSION_TOKEN" > "$HOME/.config/fluxer-token"
./9flx serve --token-file "$HOME/.config/fluxer-token"
```

To require 9P authentication, put a separate password in another mode-0600 file:

```sh
install -m 600 /dev/null "$HOME/.config/9flx-password"
printf '%s' "$YOUR_9P_PASSWORD" > "$HOME/.config/9flx-password"
./9flx serve --token-file "$HOME/.config/fluxer-token" \
    --auth-user 9flx --auth-file "$HOME/.config/9flx-password"
```

This uses the 9P authentication fid with SASL PLAIN. It prevents unauthenticated
mounts but does not encrypt the connection; use it only over a trusted network or
an encrypted tunnel. Keep the 9P password separate from the Fluxer session token.

## Mount

Linux:

```sh
sudo mkdir -p /mnt/9flx
sudo mount -t 9p -o trans=tcp,port=5640,version=9p2000,uname=9flx 127.0.0.1 /mnt/9flx
```

## Use

Each directory under `friends`, `dms`, and `communities/*/channels` contains
`history`, `pins`, `events`, `send`, `edit`, `reply`, `delete`, `react`,
`unreact`, `pin`, `unpin`, `read`, and `typing`. JSON Lines variants are
available for the read-only message and event streams.

```sh
cat history
echo 'hello' > send
echo '123456789 corrected text' > edit
echo '123456789 a reply' > reply
echo '123456789 👍' > react
echo '123456789' > pin
echo '123456789' > read
echo '123456789' > delete
: > typing
cat "$HOME/Downloads/cat.png" > send
```

To send an attachment with a filename and caption:

```sh
{
    echo '!attach cat.png'
    echo 'look at this creature'
    echo
    cat "$HOME/Downloads/cat.png"
} > send
```

`me`, friend directories, and one-to-one DMs also expose the user's profile picture as `avatar` and its CDN address as `avatar.url`.

```sh
cp avatar /tmp/avatar
cat avatar.url
```

`me/avatar` is also writable and accepts PNG, JPEG, GIF, or WebP images:

```sh
cat newpfp.png > /mnt/9flx/me/avatar
```

Read or change your presence with `me/status`:

```sh
cat me/status
echo dnd > me/status
```

The available statuses are `online`, `dnd`, `idle`, and `invisible`.

Set your custom status:

```sh
echo 'test' > me/custom-status
```

## Does it work on Plan 9?

Sure does.

![9flx running on Plan 9](screenshot.png)

Start 9flx with a listener reachable from the Plan 9 machine:

```sh
./9flx serve --listen 0.0.0.0:5640 --token-file "$HOME/.config/fluxer-token"
```

Mount it from Plan 9:

```rc
mkdir -p /mnt/9flx
aux/dial tcp!server.example!5640 mount -n -c /fd/0 /mnt/9flx
```

## License

MIT
