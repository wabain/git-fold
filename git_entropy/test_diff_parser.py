from __future__ import annotations

from typing import List, Optional

from pprint import pformat
from unittest import TestCase

from .git import Hunk, DiffLineType
from .errors import Fatal
from .diff_parser import FileDiffSummary, parse_diff_hunks, parse_diff_tree_summary


# pylint: disable=anomalous-backslash-in-string

BASIC_DIFF = b'''\
diff --git a/src/buildtime-assets/img/post-separator.svg b/src/buildtime-assets/img/post-separator.svg
new file mode 100644
index 0000000..3e3115d
--- /dev/null
+++ b/src/buildtime-assets/img/post-separator.svg
@@ -0,0 +1,3 @@
+<svg width="105px" height="4px" viewBox="0 0 103 4" version="1.1" xmlns="http://www.w3.org/2000/svg">
+    <polygon fill="#F2B632" points="3 0 103 0 100 4 0 4"></polygon>
+</svg>
\ No newline at end of file
diff --git a/src/js/dynamic-navigation.js b/src/js/dynamic-navigation.js
index 5da697f..7172ba9 100644
--- a/src/js/dynamic-navigation.js
+++ b/src/js/dynamic-navigation.js
@@ -191,12 +191,13 @@ class PageTransformer {
             frag.appendChild(temp.firstChild)
         }
 \n\
+        const oldAttrs = getContentAttributes(this.contentElem)
         const newAttrs = getContentAttributes(frag)

         this._setDocTitle(newAttrs.title)
         this._updateNavLinks({ active: href })
 \n\
-        transitionContent(this.contentElem, frag, options).catch((err) => {
+        transitionContent(this.contentElem, oldAttrs, newAttrs, frag, options).catch((err) => {
             debug('load %s: transition: fatal: %s', href, err)
             location.reload()
         })
@@ -227,13 +228,14 @@ class PageTransformer {
 \n\
 function getContentAttributes(root) {
     if (!root.children) {
-        return { title: null }
+        return { title: null, isLongform: false }
     }
 \n\
     const e = root.children[0]
 \n\
     return {
         title: e.getAttribute('data-page-meta'),
+        isLongform: e.hasAttribute('data-content-longform'),
     }
 }
 \n\
'''

ADD = DiffLineType.Add
RM = DiffLineType.Remove
CTX = DiffLineType.Context

BASIC_HUNKS = [
    Hunk(
        old_file=None,
        new_file=b'src/buildtime-assets/img/post-separator.svg',
        old_start=0,
        new_start=1,
        ops=[
            (
                ADD,
                b'<svg width="105px" height="4px" viewBox="0 0 103 4" version="1.1" xmlns="http://www.w3.org/2000/svg">\n',
            ),
            (
                ADD,
                b'    <polygon fill="#F2B632" points="3 0 103 0 100 4 0 4"></polygon>\n',
            ),
            (ADD, b'</svg>'),
        ],
    ),
    Hunk(
        old_file=b'src/js/dynamic-navigation.js',
        new_file=b'src/js/dynamic-navigation.js',
        old_start=191,
        new_start=191,
        ops=[
            (CTX, b'            frag.appendChild(temp.firstChild)\n'),
            (CTX, b'        }\n'),
            (CTX, b'\n'),
            (ADD, b'        const oldAttrs = getContentAttributes(this.contentElem)\n'),
            (CTX, b'        const newAttrs = getContentAttributes(frag)\n'),
            (CTX, b'        this._setDocTitle(newAttrs.title)\n'),
            (CTX, b'        this._updateNavLinks({ active: href })\n'),
            (CTX, b'\n'),
            (
                RM,
                b'        transitionContent(this.contentElem, frag, options).catch((err) => {\n',
            ),
            (
                ADD,
                b'        transitionContent(this.contentElem, oldAttrs, newAttrs, frag, options).catch((err) => {\n',
            ),
            (CTX, b"            debug('load %s: transition: fatal: %s', href, err)\n"),
            (CTX, b'            location.reload()\n'),
            (CTX, b'        })\n'),
        ],
    ),
    Hunk(
        old_file=b'src/js/dynamic-navigation.js',
        new_file=b'src/js/dynamic-navigation.js',
        old_start=227,
        new_start=228,
        ops=[
            (CTX, b'\n'),
            (CTX, b'function getContentAttributes(root) {\n'),
            (CTX, b'    if (!root.children) {\n'),
            (RM, b'        return { title: null }\n'),
            (ADD, b'        return { title: null, isLongform: false }\n'),
            (CTX, b'    }\n'),
            (CTX, b'\n'),
            (CTX, b'    const e = root.children[0]\n'),
            (CTX, b'\n'),
            (CTX, b'    return {\n'),
            (CTX, b"        title: e.getAttribute('data-page-meta'),\n"),
            (ADD, b"        isLongform: e.hasAttribute('data-content-longform'),\n"),
            (CTX, b'    }\n'),
            (CTX, b'}\n'),
            (CTX, b'\n'),
        ],
    ),
]


BASIC_TREE_DIFF = b'''\
:100755 100755 6041d9c9bd255c62d1595b90aee27026a103771b 3649362affc209c1663c6c42e6f6c497b1395011 R091\tbin/deploy.sh\tbin/ci-deploy.sh
:100644 100644 0fb25ba9892e0f186e7534441b81ff977c7ec349 c5cfa2405dff2d44998f8c682df20bc89b4ec1f2 M\tcontent/index.html
:100644 100644 019a95298b6eb865e11958ac2f1a24ced08d15c5 e318a830d437fd87facf5f31052252fa94ff39e0 M\tpackage.json
:000000 100644 0000000000000000000000000000000000000000 a81d9b6ffd480c64c64aef42af665d58eea4fe61 A\tsrc/buildtime-assets/img/grid-bg-center.svg
:000000 100644 0000000000000000000000000000000000000000 c03fc19924949e77ca8e04f7b8a71e1b0766c87e A\tsrc/buildtime-assets/img/grid-bg.svg
:100644 000000 96e3f21b8a127104638336622185c748f52eb478 0000000000000000000000000000000000000000 D\tsrc/scss/_header.scss
:000000 100644 0000000000000000000000000000000000000000 6cbfa9da41a631f2ada656dc98b029596593d02c A\tsrc/scss/sections/_about.scss
:000000 100644 0000000000000000000000000000000000000000 33ad7659450bba4f89ed2a258a7db3db91616d0f A\tsrc/scss/sections/_header.scss
:000000 100644 0000000000000000000000000000000000000000 cbee3503a423cf654c414967cb1662e167f49efb A\tsrc/scss/sections/_legacy-page.scss
'''

EXPECTED_BASIC_TREE_SUMMARY = [
    FileDiffSummary(
        old_mode='100755',
        new_mode='100755',
        old_oid='6041d9c9bd255c62d1595b90aee27026a103771b',
        new_oid='3649362affc209c1663c6c42e6f6c497b1395011',
        delta_type='R',
        similarity=91,
        old_path=b'bin/deploy.sh',
        new_path=b'bin/ci-deploy.sh',
    ),
    FileDiffSummary(
        old_mode='100644',
        new_mode='100644',
        old_oid='0fb25ba9892e0f186e7534441b81ff977c7ec349',
        new_oid='c5cfa2405dff2d44998f8c682df20bc89b4ec1f2',
        delta_type='M',
        similarity=None,
        old_path=b'content/index.html',
        new_path=b'content/index.html',
    ),
    FileDiffSummary(
        old_mode='100644',
        new_mode='100644',
        old_oid='019a95298b6eb865e11958ac2f1a24ced08d15c5',
        new_oid='e318a830d437fd87facf5f31052252fa94ff39e0',
        delta_type='M',
        similarity=None,
        old_path=b'package.json',
        new_path=b'package.json',
    ),
    FileDiffSummary(
        old_mode='000000',
        new_mode='100644',
        old_oid='0000000000000000000000000000000000000000',
        new_oid='a81d9b6ffd480c64c64aef42af665d58eea4fe61',
        delta_type='A',
        similarity=None,
        old_path=None,
        new_path=b'src/buildtime-assets/img/grid-bg-center.svg',
    ),
    FileDiffSummary(
        old_mode='000000',
        new_mode='100644',
        old_oid='0000000000000000000000000000000000000000',
        new_oid='c03fc19924949e77ca8e04f7b8a71e1b0766c87e',
        delta_type='A',
        similarity=None,
        old_path=None,
        new_path=b'src/buildtime-assets/img/grid-bg.svg',
    ),
    FileDiffSummary(
        old_mode='100644',
        new_mode='000000',
        old_oid='96e3f21b8a127104638336622185c748f52eb478',
        new_oid='0000000000000000000000000000000000000000',
        delta_type='D',
        similarity=None,
        old_path=b'src/scss/_header.scss',
        new_path=None,
    ),
    FileDiffSummary(
        old_mode='000000',
        new_mode='100644',
        old_oid='0000000000000000000000000000000000000000',
        new_oid='6cbfa9da41a631f2ada656dc98b029596593d02c',
        delta_type='A',
        similarity=None,
        old_path=None,
        new_path=b'src/scss/sections/_about.scss',
    ),
    FileDiffSummary(
        old_mode='000000',
        new_mode='100644',
        old_oid='0000000000000000000000000000000000000000',
        new_oid='33ad7659450bba4f89ed2a258a7db3db91616d0f',
        delta_type='A',
        similarity=None,
        old_path=None,
        new_path=b'src/scss/sections/_header.scss',
    ),
    FileDiffSummary(
        old_mode='000000',
        new_mode='100644',
        old_oid='0000000000000000000000000000000000000000',
        new_oid='cbee3503a423cf654c414967cb1662e167f49efb',
        delta_type='A',
        similarity=None,
        old_path=None,
        new_path=b'src/scss/sections/_legacy-page.scss',
    ),
]


class ParseDiffHunksTest(TestCase):

    maxDiff = None

    def test_diff_hunk_basic(self) -> None:
        hunks = list(parse_diff_hunks(BASIC_DIFF))
        self.assertEqual([h.ops for h in BASIC_HUNKS], [h.ops for h in hunks])
        self.assertEqual(BASIC_HUNKS, hunks)

    def test_diff_hunk_malformed(self) -> None:
        diff_lines = [
            # 0
            b'diff --git a/test_file.txt b/test_file.txt\n',
            # 1
            b'new file mode 100644\n',
            # 2
            b'index 0000000..3e3115d\n',
            # 3
            b'--- /dev/null\n',
            # 4
            b'+++ b/test_file.txt\n',
            # 5
            b'@@ -0,0 +1,1 @@\n',
            # 6
            b'+Testing!\n',
        ]

        with self.subTest('drop /dev/null line'):
            self.assert_diff_parse_fails_on_line(5, diff_lines[:3] + diff_lines[4:])

        with self.subTest('drop new filename line'):
            self.assert_diff_parse_fails_on_line(5, diff_lines[:4] + diff_lines[5:])

        with self.subTest('duplicate new filename line'):
            self.assert_diff_parse_fails_on_line(
                6, diff_lines[:4] + ([diff_lines[4]] * 2) + diff_lines[5:]
            )

        with self.subTest('missing header line'):
            self.assert_diff_parse_fails_on_line(
                1, diff_lines[1:], msg='unable to locate diff content'
            )

        with self.subTest('diff truncated'):
            self.assert_diff_parse_fails_on_line(
                6,
                diff_lines[:4] + [diff_lines[4].rstrip(b'\n')],
                msg='unexpected end of diff',
            )

        with self.subTest('weird line in header'):
            self.assert_diff_parse_fails_on_line(
                2, [diff_lines[0], b'Weird!\n'] + diff_lines[1:]
            )

        with self.subTest('no-newline in empty diff'):
            self.assert_diff_parse_fails_on_line(
                7, diff_lines[:6] + [b'\\ No newline at end of file\n']
            )

        with self.subTest('duplicate no-newline'):
            self.assert_diff_parse_fails_on_line(
                9, diff_lines + [b'\\ No newline at end of file\n'] * 2
            )

        with self.subTest('diff line with invalid leading character'):
            self.assert_diff_parse_fails_on_line(
                7, diff_lines[:6] + [b'?' + diff_lines[6][1:]]
            )

    def assert_diff_parse_fails_on_line(
        self, failure_line: int, lines: List[bytes], msg: Optional[str] = None
    ) -> None:
        try:
            hunks = list(parse_diff_hunks(b''.join(lines)))
        except Fatal as exc:
            if msg is None:
                msg = f'unexpected diff content at line {failure_line}'

            self.assertEqual(msg, str(exc))
            return

        assert False, f'Expected a diff parsing error, got hunks: {pformat(hunks)}'


class ParseDiffTreeTest(TestCase):

    maxDiff = None

    def test_diff_tree_basic(self) -> None:
        summary = list(parse_diff_tree_summary(BASIC_TREE_DIFF))
        self.assertEqual(EXPECTED_BASIC_TREE_SUMMARY, summary)

    def test_diff_tree_malformed_line(self) -> None:
        with self.assertRaisesRegex(Fatal, 'unable to parse diff-tree output line 1'):
            parse_diff_tree_summary(b'????\n')
