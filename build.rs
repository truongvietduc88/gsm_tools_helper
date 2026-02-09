fn main() {
    // DuckDB (bundled) on Windows needs Restart Manager library for some functions
    // (RmStartSession, RmEndSession, RmRegisterResources, RmGetList)
    println!("cargo:rustc-link-lib=Rstrtmgr");
}
