//! Checks the code snippets in the Markdown docs.
//!
//! Rustdoc compiles the Rust snippets of each file in `DOCTESTED`. This
//! test makes sure that this stays true, and it checks what rustdoc does
//! not check:
//!
//! - Each `toml` snippet is valid TOML.
//! - Each dependency pin in a Markdown file names the same version series
//!   as the manifests of this repository. A stale `autumn-web = "0.9"`
//!   in a README fails this test.
//! - A Rust snippet in a doctested file does not use `ignore`. Use
//!   `no_run` for code that must compile but must not run. An excerpt is
//!   the one exception: its first line is ``// From `path`.``, and each of
//!   its code lines must be in that file, which CI compiles.
//!
//! Keep this file the same in all Autumn plugin repositories. Only the
//! constants in the "Repository settings" part change.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

// Repository settings.

/// Markdown files that rustdoc compiles, relative to the repository root.
const DOCTESTED: &[&str] = &["README.md"];

/// Paths that the scan skips, relative to the repository root. Decision
/// records keep the versions that were current when they were written.
const SKIPPED: &[&str] = &["docs/adr", "CHANGELOG.md"];

// End of repository settings.

/// Directories that never hold project docs.
const SKIPPED_DIRS: &[&str] = &["target", "node_modules", ".git", "vendor"];

/// The fence info words that make rustdoc skip a Rust snippet.
const SKIP_WORDS: &[&str] = &["ignore"];

/// Dependency tables in a manifest.
const DEP_TABLES: &[&str] = &["dependencies", "dev-dependencies", "build-dependencies"];

#[test]
fn doc_snippets_are_current() {
    let root = repo_root();
    let known = known_series(&root);
    let mut problems = String::new();

    for file in markdown_files(&root) {
        let rel = relative(&root, &file);
        let Ok(text) = std::fs::read_to_string(&file) else {
            let _ = writeln!(problems, "{rel}: cannot read the file");
            continue;
        };
        let doctested = DOCTESTED.contains(&rel.as_str());
        let read = |path: &str| std::fs::read_to_string(root.join(path)).ok();
        for problem in check_markdown(&text, doctested, &known, &read) {
            let _ = writeln!(problems, "{rel}:{problem}");
        }
    }

    assert!(
        problems.is_empty(),
        "stale or broken doc snippets:\n{problems}"
    );
}

#[test]
fn doctested_files_are_included_by_rustdoc() {
    let root = repo_root();
    let lib = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    let source = std::fs::read_to_string(&lib).unwrap_or_default();
    for file in DOCTESTED {
        assert!(
            root.join(file).is_file(),
            "{file} is in DOCTESTED but does not exist"
        );
        let stem = Path::new(file)
            .file_stem()
            .and_then(|n| n.to_str())
            .unwrap_or(file);
        // The path is a literal or an `env!` name, as in `env!("X_README")`.
        assert!(
            source
                .split("include_str!(")
                .skip(1)
                .any(|rest| rest.split(')').next().is_some_and(|arg| arg.contains(stem))),
            "src/lib.rs does not include {file} with include_str!, so rustdoc does not compile it"
        );
    }
}

/// Finds the problems in one Markdown file. Each problem starts with a
/// line number.
fn check_markdown(
    text: &str,
    doctested: bool,
    known: &BTreeMap<String, BTreeSet<String>>,
    read: &dyn Fn(&str) -> Option<String>,
) -> Vec<String> {
    let mut problems = Vec::new();

    for block in fenced_blocks(text) {
        let words: Vec<&str> = block
            .info
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|w| !w.is_empty())
            .collect();
        let lang = words.first().copied().unwrap_or("");
        if lang == "toml"
            && let Err(error) = block.body.parse::<toml::Table>()
        {
            let message = error.message().to_owned();
            problems.push(format!("{}: invalid TOML snippet: {message}", block.line));
        }
        if doctested && lang == "rust" && words.iter().any(|w| SKIP_WORDS.contains(w)) {
            problems.extend(check_excerpt(&block, read));
        }
    }

    for (line, name, requirement) in pins(text) {
        let Some(series) = known.get(&name) else {
            continue;
        };
        let Some(pinned) = series_of(&requirement) else {
            continue;
        };
        if !series.contains(&pinned) {
            let want: Vec<&str> = series.iter().map(String::as_str).collect();
            problems.push(format!(
                "{line}: `{name} = \"{requirement}\"` is series {pinned}, the manifests use {}",
                want.join(" or ")
            ));
        }
    }

    problems
}

/// Checks a Rust snippet that rustdoc skips. It must be an excerpt: its
/// first line names the source files, and each code line is in one of them.
fn check_excerpt(block: &Block, read: &dyn Fn(&str) -> Option<String>) -> Vec<String> {
    let first = block.body.lines().next().unwrap_or("").trim();
    let sources: Vec<&str> = first
        .strip_prefix("// From ")
        .map(|rest| rest.split('`').skip(1).step_by(2).collect())
        .unwrap_or_default();
    if sources.is_empty() {
        return vec![format!(
            "{}: rustdoc skips this snippet (`{}`); make it compile, use `no_run`, \
             or start the excerpt with ``// From `path`.``",
            block.line, block.info
        )];
    }
    let mut source_lines = BTreeSet::new();
    let mut problems = Vec::new();
    for source in &sources {
        match read(source) {
            Some(text) => source_lines.extend(text.lines().map(|l| l.trim().to_owned())),
            None => problems.push(format!(
                "{}: excerpt source `{source}` does not exist",
                block.line
            )),
        }
    }
    for (offset, line) in block.body.lines().enumerate() {
        let code = line.trim();
        if !code.is_empty() && !code.starts_with("//") && !source_lines.contains(code) {
            problems.push(format!(
                "{}: excerpt line is not in {}: {code}",
                block.line + 1 + offset,
                sources.join(", ")
            ));
        }
    }
    problems
}

/// A fenced code block.
#[derive(Debug)]
struct Block {
    /// The 1-based line of the opening fence.
    line: usize,
    /// The info string after the opening fence.
    info: String,
    /// The lines between the fences, without the indent of the fence.
    body: String,
}

/// Reads the fenced code blocks of a Markdown text.
fn fenced_blocks(text: &str) -> Vec<Block> {
    let mut blocks = Vec::new();
    let mut open: Option<(usize, String, usize, String, Block)> = None;
    for (index, raw) in text.lines().enumerate() {
        let indent = raw.len() - raw.trim_start().len();
        let line = raw.trim_start().trim_start_matches('>').trim_start();
        let fence: String = line.chars().take_while(|&c| c == '`' || c == '~').collect();
        let is_fence = fence.len() >= 3
            && fence
                .chars()
                .all(|c| c == fence.chars().next().unwrap_or('`'));
        match open.as_mut() {
            None if is_fence => {
                let info = line[fence.len()..].trim().to_owned();
                let block = Block {
                    line: index + 1,
                    info: info.clone(),
                    body: String::new(),
                };
                open = Some((indent, fence, index, info, block));
            }
            None => {}
            Some((_, marker, _, _, _))
                if is_fence
                    && fence.len() >= marker.len()
                    && line[fence.len()..].trim().is_empty() =>
            {
                if let Some((_, _, _, _, block)) = open.take() {
                    blocks.push(block);
                }
            }
            Some((fence_indent, _, _, _, block)) => {
                let cut = raw.len() - raw.trim_start().len();
                let body = &raw[cut.min(*fence_indent)..];
                let body = body
                    .strip_prefix('>')
                    .map_or(body, |b| b.strip_prefix(' ').unwrap_or(b));
                block.body.push_str(body);
                block.body.push('\n');
            }
        }
    }
    blocks
}

/// Finds `name = "req"` and `name = { version = "req" ... }` pins.
/// Returns the 1-based line, the crate name and the requirement.
fn pins(text: &str) -> Vec<(usize, String, String)> {
    let mut found = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let mut rest = line;
        while let Some(eq) = rest.find('=') {
            let before = rest[..eq].trim_end();
            let name: String = before
                .chars()
                .rev()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                .collect::<String>()
                .chars()
                .rev()
                .collect();
            let after = rest[eq + 1..].trim_start();
            if !name.is_empty()
                && name != "version"
                && let Some(requirement) = requirement_of(after)
            {
                found.push((index + 1, name, requirement));
            }
            rest = &rest[eq + 1..];
        }
    }
    found
}

/// Reads the version requirement at the start of a TOML value.
fn requirement_of(value: &str) -> Option<String> {
    if let Some(quoted) = value.strip_prefix('"') {
        return quoted.split('"').next().map(str::to_owned);
    }
    let table = value.strip_prefix('{')?;
    let table = table.split('}').next()?;
    let at = table.find("version")?;
    let after = table[at + "version".len()..]
        .trim_start()
        .strip_prefix('=')?;
    let after = after.trim_start().strip_prefix('"')?;
    after.split('"').next().map(str::to_owned)
}

/// Gives the semver series of a requirement: `0.10` for `0.10.2` or
/// `>=0.10, <0.11`, and `1` for `1.4`. Gives `None` without a number.
fn series_of(requirement: &str) -> Option<String> {
    let start = requirement.find(|c: char| c.is_ascii_digit())?;
    let version: String = requirement[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let mut parts = version.split('.').filter(|p| !p.is_empty());
    let major: u64 = parts.next()?.parse().ok()?;
    if major > 0 {
        return Some(major.to_string());
    }
    let Some(minor) = parts.next() else {
        return Some("0".to_owned());
    };
    let minor: u64 = minor.parse().ok()?;
    if minor > 0 {
        return Some(format!("0.{minor}"));
    }
    let patch = parts.next().map_or(Ok(0), str::parse::<u64>).ok()?;
    Some(format!("0.0.{patch}"))
}

/// Collects the version series of each crate that a manifest in this
/// repository names, and of each package in it.
fn known_series(root: &Path) -> BTreeMap<String, BTreeSet<String>> {
    let mut known: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for manifest in files(root, &|p| p.file_name().is_some_and(|n| n == "Cargo.toml")) {
        let doc = std::fs::read_to_string(&manifest)
            .ok()
            .and_then(|text| text.parse::<toml::Table>().ok());
        let Some(doc) = doc else {
            continue;
        };
        let workspace = doc.get("workspace").and_then(toml::Value::as_table);
        let workspace_version = workspace
            .and_then(|w| w.get("package"))
            .and_then(|p| p.get("version"))
            .and_then(toml::Value::as_str);

        if let Some(package) = doc.get("package").and_then(toml::Value::as_table) {
            let version = match package.get("version") {
                Some(toml::Value::String(v)) => Some(v.as_str()),
                Some(toml::Value::Table(_)) => workspace_version,
                _ => None,
            };
            if let (Some(name), Some(series)) = (
                package.get("name").and_then(toml::Value::as_str),
                version.and_then(series_of),
            ) {
                known.entry(name.to_owned()).or_default().insert(series);
            }
        }

        let mut tables: Vec<&toml::Table> = Vec::new();
        for key in DEP_TABLES {
            tables.extend(doc.get(*key).and_then(toml::Value::as_table));
        }
        if let Some(w) = workspace {
            tables.extend(w.get("dependencies").and_then(toml::Value::as_table));
        }
        if let Some(targets) = doc.get("target").and_then(toml::Value::as_table) {
            for target in targets.values().filter_map(toml::Value::as_table) {
                for key in DEP_TABLES {
                    tables.extend(target.get(*key).and_then(toml::Value::as_table));
                }
            }
        }
        for table in tables {
            for (name, value) in table {
                let requirement = match value {
                    toml::Value::String(r) => Some(r.as_str()),
                    toml::Value::Table(t) => t.get("version").and_then(toml::Value::as_str),
                    _ => None,
                };
                let name = match value {
                    toml::Value::Table(t) => t
                        .get("package")
                        .and_then(toml::Value::as_str)
                        .unwrap_or(name),
                    _ => name,
                };
                if let Some(series) = requirement.and_then(series_of) {
                    known.entry(name.to_owned()).or_default().insert(series);
                }
            }
        }
    }
    known
}

/// Lists the Markdown files that the scan reads.
fn markdown_files(root: &Path) -> Vec<PathBuf> {
    files(root, &|p| p.extension().is_some_and(|e| e == "md"))
        .into_iter()
        .filter(|p| {
            let rel = relative(root, p);
            !SKIPPED
                .iter()
                .any(|s| rel == *s || rel.starts_with(&format!("{s}/")))
        })
        .collect()
}

/// Lists the files below `root` that match `keep`, in a stable order.
fn files(root: &Path, keep: &dyn Fn(&Path) -> bool) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            if path.is_dir() {
                let skipped = name.to_str().is_some_and(|n| SKIPPED_DIRS.contains(&n));
                if !skipped {
                    stack.push(path);
                }
            } else if keep(&path) {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

/// Finds the repository root: the nearest parent with `.git`.
fn repo_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .find(|dir| dir.join(".git").exists())
        .map_or_else(|| manifest_dir.clone(), Path::to_path_buf)
}

/// Gives a path relative to the root, with `/` separators.
fn relative(root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn series_follows_semver() {
    assert_eq!(series_of("0.10").as_deref(), Some("0.10"));
    assert_eq!(series_of("0.10.3").as_deref(), Some("0.10"));
    assert_eq!(series_of(">=0.8, <0.9").as_deref(), Some("0.8"));
    assert_eq!(series_of("^1.4").as_deref(), Some("1"));
    assert_eq!(series_of("0.0.4").as_deref(), Some("0.0.4"));
    assert_eq!(series_of("...").as_deref(), None);
}

#[test]
fn pins_read_both_forms() {
    let text =
        "autumn-web = \"0.9\"\nfoo = { version = \"1.2\", features = [\"x\"] }\nversion = \"3\"\n";
    let found = pins(text);
    assert_eq!(
        found,
        vec![
            (1, "autumn-web".to_owned(), "0.9".to_owned()),
            (2, "foo".to_owned(), "1.2".to_owned()),
        ]
    );
}

#[test]
fn a_stale_pin_fails() {
    let known = BTreeMap::from([("autumn-web".to_owned(), BTreeSet::from(["0.10".to_owned()]))]);
    let text = "```toml\n[dependencies]\nautumn-web = \"0.9\"\n```\n";
    let problems = check_markdown(text, false, &known, &|_| None);
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(problems[0].starts_with("3: "), "{problems:?}");
    assert_eq!(
        check_markdown(&text.replace("0.9", "0.10"), false, &known, &|_| None),
        Vec::<String>::new()
    );
}

#[test]
fn broken_toml_and_ignored_rust_fail() {
    let known = BTreeMap::new();
    let text = "```toml\n[x\n```\n\n```rust,ignore\nfn main() {}\n```\n";
    assert_eq!(check_markdown(text, true, &known, &|_| None).len(), 2);
    assert_eq!(check_markdown(text, false, &known, &|_| None).len(), 1);
}

#[test]
fn an_excerpt_must_match_its_source() {
    let known = BTreeMap::new();
    let source =
        |path: &str| (path == "src/a.rs").then(|| "fn a() {\n    let x = 1;\n}\n".to_owned());
    let good = "```rust,ignore\n// From `src/a.rs`.\nlet x = 1;\n```\n";
    assert_eq!(
        check_markdown(good, true, &known, &source),
        Vec::<String>::new()
    );
    let stale = good.replace("let x = 1;", "let x = 2;");
    assert_eq!(check_markdown(&stale, true, &known, &source).len(), 1);
    let missing = good.replace("src/a.rs", "src/b.rs");
    assert_eq!(check_markdown(&missing, true, &known, &source).len(), 2);
}
