@echo off
setlocal

set "SRC_DIR=%~dp0skills"
set "TARGET_DIR=%USERPROFILE%\.codex\skills"

if not exist "%TARGET_DIR%" mkdir "%TARGET_DIR%"
xcopy "%SRC_DIR%\zhongdong-congling-gongcheng" "%TARGET_DIR%\zhongdong-congling-gongcheng" /E /I /Y >nul
xcopy "%SRC_DIR%\zhongdong-zongskill-anzhuang-diaoyong" "%TARGET_DIR%\zhongdong-zongskill-anzhuang-diaoyong" /E /I /Y >nul

echo 技能安装完成：
echo - %TARGET_DIR%\zhongdong-congling-gongcheng
echo - %TARGET_DIR%\zhongdong-zongskill-anzhuang-diaoyong
echo 请重新打开AI会话后再触发技能。
endlocal
