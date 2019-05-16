## ImplTest

ImplTest is a test project combining [AdaptiveCEP](http://github.com/pweisenburger/AdaptiveCEP) and [SecureScala](http://github.com/allprojects/securescala)

## Building

- Build and install http://github.com/allprojects/securescala/tree/libope
- Run `sbt run` to start a simulation

## Updating dependencies

Both, AdaptiveCEP and SecureScala are included directly from Github, so if you want to update your local version after building the project once, clean the staging folder:

```
rm -R ~/.sbt/1.0/staging/
```