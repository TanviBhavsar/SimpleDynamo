package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

/**
 * Created by tanvi on 4/16/15.
 */
public class OnStarClick implements View.OnClickListener {
    private static final String TAG = OnTestClickListener.class.getName();
    private static final int TEST_CNT = 2;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;
    private final ContentValues[] mContentValues;

    public OnStarClick(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        mContentValues = initTestValues();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private ContentValues[] initTestValues() {
        ContentValues[] cv = new ContentValues[TEST_CNT];
        for (int i = 0; i < TEST_CNT; i++) {
            cv[i] = new ContentValues();
            cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
            cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
        }

        return cv;
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            if (testInsert()) {
                publishProgress("Insert success\n");
            } else {
                publishProgress("Insert fail\n");
                return null;
            }

            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Cursor resultCursor = testQuery();

            if (resultCursor != null) {
                //publishProgress("Query success\n");
                StringBuilder resultQuery = new StringBuilder();
                while (resultCursor.moveToNext()) {
                    resultQuery.append("\n" + resultCursor.getString(0) + " " + resultCursor.getString(1));
                }
                if (resultQuery.length() == 0)
                    resultQuery.append("Empty");
                publishProgress(resultQuery.toString());

            } else {
                publishProgress("Resultcursor is null..Query fail\n");
            }

            int a=testDelete();
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            mTextView.append(strings[0]);

            return;
        }

        private boolean testInsert() {
            try {
                for (int i = 0; i < TEST_CNT; i++) {
                    mContentResolver.insert(mUri, mContentValues[i]);
                }
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }

        private Cursor testQuery() {

            Cursor resultCursor = null;
            try {


                Log.e(TAG, "in test query");
                String starkey = "\"@\"";
                resultCursor = mContentResolver.query(mUri, null,
                        starkey, null, null);
                //    resultCursor.
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }

            } catch (Exception e) {
                return null;
            }

            return resultCursor;
        }


        private int testDelete() {


            int r=0;
            Cursor resultCursor = null;
            try {


                Log.e(TAG, "in test delete");
                String starkey = "\"@\"";


                r = mContentResolver.delete(mUri, starkey, null);
                //    resultCursor.
               /* if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }*/

            } catch (Exception e) {
                return r;
            }

            return r;
        }
    }

}

