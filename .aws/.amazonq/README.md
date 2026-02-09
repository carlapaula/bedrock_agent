# Renovação de Sessão SSO

## Quando a sessão SSO expirar

Execute o comando abaixo para renovar:

```bash
aws sso login --profile sso
```

Isso abrirá seu navegador para autenticação. Após confirmar, a sessão será renovada.

## Verificar status da sessão

```bash
aws sts get-caller-identity --profile sso
```

Se retornar erro, a sessão expirou e precisa ser renovada.
