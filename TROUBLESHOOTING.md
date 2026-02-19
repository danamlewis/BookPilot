# Troubleshooting Guide

## Catalog Fetch Issues

### URL Construction Errors

If you see errors like:
```
HTTPSConnectionPool(host='openlibrary.orgol3801348a', port=443)
```

This has been fixed in the latest code. The issue was malformed URLs. 

**Solution:** The code now properly normalizes author IDs. If you still see this:
1. Clear any cached author IDs: The database might have incorrectly formatted IDs
2. Re-run the catalog command - it will now fix the IDs automatically

### Network/SSL Errors

If you see SSL or connection errors:
- Check your internet connection
- Some corporate networks block API calls
- Try running again - the code now handles errors gracefully and continues

### Process Takes Too Long

The catalog fetch can take 10-30 minutes for many authors because:
- API rate limits (0.5 second delay between calls)
- Each author requires multiple API calls

**Tips:**
- Let it run in the background
- You can stop it (Ctrl+C) and restart - it will continue where it left off
- Already processed authors won't be refetched for 7 days

### No Results for Some Authors

Some authors might not be found in Open Library or Google Books:
- This is normal - not all authors are in these databases
- The process will continue with other authors
- Check the error list at the end to see which authors failed

## Database Issues

### "Database is locked"

This happens if:
- The web server is running (it holds a database connection)
- Another process is using the database

**Solution:**
- Stop the web server (`python web/app.py`) before running CLI commands
- Or use separate database files for CLI and web

### "No such table"

If you see table errors:
- The database might be corrupted or old
- Delete `data/bookpilot.db` and re-run `ingest`

## Web UI Issues

### Page Shows Zeros

- Make sure you've run `ingest` first
- Check that the CSV file was parsed correctly
- Run `python scripts/bookpilot.py status` to verify data

### No Recommendations

- You need to run `catalog` command first
- This fetches author catalogs from APIs
- Without catalogs, there's nothing to recommend

### Recommendations Are Empty

- Check that catalog fetch completed successfully
- Some authors might not have catalogs available
- Try running `catalog` again with `--force` flag

## General Tips

### Check Status First

Always start with:
```bash
python scripts/bookpilot.py status
```

This shows:
- How many books/authors you have
- When data was last imported
- When catalogs were last checked

### Clear Cache

If API calls seem stuck or return old data:
```bash
rm -rf data/cache/*
```

Then re-run `catalog` command.

### Start Fresh

If everything seems broken:
1. Backup your CSV file
2. Delete `data/bookpilot.db`
3. Delete `data/cache/`
4. Re-run `ingest`
5. Re-run `catalog`
